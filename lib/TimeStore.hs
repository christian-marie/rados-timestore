--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-# OPTIONS -cpp #-}

module TimeStore
(
    -- * Types
    Time(..),
    Address(..),
    NameSpace(unNameSpace),
    nameSpace,
    Store(..),
    Bucket,

    -- * Writing
    writeEncoded,

    -- * Reading
    readSimple,
    readExtended,

    -- * Utility/internal
    registerNamespace,
    isRegistered,
    fetchIndex,

#if defined RADOS
    -- * Rados store (ceph)
    RadosStore(..),
    radosStore,
    cleanupRadosStore,
    withRadosStore,
#endif

    -- * Memory store (testing)
    MemoryStore,
    memoryStore,
    dumpMemoryStore,
) where

import Control.Applicative
import Control.Concurrent.Async
import Control.Exception
import Control.Lens hiding (Index, Simple, each, index)
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Data.Maybe
import Data.Tagged
import Data.Tuple.Sequence (sequenceT)
import Pipes
import qualified Pipes.Prelude as P
import TimeStore.Algorithms
import TimeStore.Core
import TimeStore.Index
import TimeStore.StoreHelpers
import TimeStore.Stores.Memory
#if defined RADOS
import TimeStore.Stores.Rados
#endif

-- | Check if a namespace is registered.
isRegistered :: Store s => s -> NameSpace -> IO Bool
isRegistered s ns =
    isJust <$> (fetchIndex s ns :: IO (Maybe (Tagged Simple Index)))

-- | Register a namespace with the given number of buckets. This is idempotent,
-- however on subsequent runs the bucket argument will be ignored.
registerNamespace :: Store s
                  => s
                  -> NameSpace
                  -> Bucket
                  -- ^ Number of simple buckets to distribute over
                  -> Bucket
                  -- ^ Number of extended buckets to distribute over
                  -> IO ()
registerNamespace s ns (Bucket s_buckets) (Bucket e_buckets) = do
    registered <- isRegistered s ns
    unless registered $
        append s ns [ ( name (undefined :: Tagged Simple Index)
                      , indexEntry 0 s_buckets)
                    , ( name (undefined :: Tagged Extended Index)
                      , indexEntry 0 e_buckets)
                    ]

-- | Write a mixed blob of points to the supplied store and namespace.
writeEncoded :: Store s => s -> NameSpace -> ByteString -> IO ()
writeEncoded s ns encoded = do
    -- The write is done within a shared lock to ensure that when a rollover
    -- occurs we have the latest index, writing with an old version of the
    -- index could result in data loss.
    ( s_idx,
      e_idx,
      s_offsets,
      e_offsets,
      s_latest,
      e_latest ) <- withSharedLock s ns "write_lock" $ do
        -- Grab the latest index, a rollover cannot occur whilst we hold the
        -- shared lock.
        (s_idx, e_idx) <- concurrently (mustFetchIndex s ns)
                                       (mustFetchIndex s ns)

        -- Collate points into epochs and buckets
        (s_writes, e_writes, p_writes,
             s_latest, e_latest) <- case groupMixed s_idx e_idx encoded of
                                        Left e -> throwIO . InvalidPayload $ e
                                        Right x -> return x

        -- Merge our latest with everyone's latest.
        (s_latest', e_latest') <- updateLatest s ns s_latest e_latest

        e_offsets <- writeBuckets s ns s_writes e_writes p_writes

        -- Now check the sizes of those buckets so that we can decide if we
        -- want to trigger a rollover
        s_offsets <- unsafePartsOf (itraversed . withIndex)
                                   (getOffsets s ns SimpleBucketLocation)
                                   s_writes

        return (s_idx, e_idx, s_offsets, e_offsets, s_latest', e_latest')


    -- Now that the lock has been released, try to do the rollover.
    let threshold = rolloverThreshold s ns
    maybeRollover s ns threshold s_offsets s_latest s_idx
    maybeRollover s ns threshold e_offsets e_latest e_idx

-- | Request a range of simple points at the given addresses, returns a
-- producer of chunks of points, each chunk is non-overlapping and ordered,
-- each point within a chunk is also ordered.
readSimple :: Store s
           => s
           -> NameSpace
           -> Time
           -> Time
           -> [Address]
           -> Producer ByteString IO ()
readSimple s ns start end addrs = do
    (Tagged s_ix) :: Tagged Simple Index <- lift (mustFetchIndex s ns)
    let objs = targetObjs s_ix start end addrs (name . SimpleBucketLocation)

    streamObjects s ns objs
    >-> P.concat
    >-> P.map (processSimple start end addrs)
    >-> P.filter (not . S.null)

-- | Request a range of simple points at the given addresses, returns a
-- producer of chunks of points, each chunk is non-overlapping and ordered,
-- each point within a chunk is also ordered.
readExtended :: Store s
           => s
           -> NameSpace
           -> Time
           -> Time
           -> [Address]
           -> Producer ByteString IO ()
readExtended s ns start end addrs = do
    (Tagged e_ix) :: Tagged Extended Index <- lift (mustFetchIndex s ns)
    let s_objs = targetObjs e_ix start end addrs (name . SimpleBucketLocation)
    let e_objs = targetObjs e_ix start end addrs (name . ExtendedBucketLocation)

    P.zip (streamObjects s ns s_objs) (streamObjects s ns e_objs)
    >-> P.map sequenceT  -- Ensure both or neither are there.
    >-> P.concat         -- Maybe (s_bs,e_bs) -> (s_bs,e_bs.)
    >-> P.map (uncurry (processExtended start end addrs))
    >-> P.filter (not . S.null)
