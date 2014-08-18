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

module TimeStore
(
    -- * Types
    Time(..),
    Address(..),
    NameSpace(..),
    Store(..),

    -- * Writing
    writeEncoded,

    -- * Reading
    readSimple,
    readExtended,

    -- * Utility/internal
    registerNamespace,
    isRegistered,
    fetchIndexes,

    -- * Memory store (testing)
    MemoryStore,
    memoryStore,
    dumpMemoryStore,
) where

import Control.Applicative
import Control.Lens hiding (Index, Simple, each, index)
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Data.Map.Strict (Map)
import Data.Maybe
import Data.Packer
import Data.Tagged
import Data.Tuple.Sequence (sequenceT)
import Data.Word (Word64)
import Pipes
import qualified Pipes.Prelude as P
import TimeStore.Algorithms
import TimeStore.Core
import TimeStore.Index
import TimeStore.StoreHelpers
import TimeStore.Stores.Memory

-- | Check if a namespace is registered.
isRegistered :: Store s => s -> NameSpace -> IO Bool
isRegistered s ns = isJust <$> fetchIndexes s ns

-- | Register a namespace with the given number of buckets. This is idempotent,
-- however on subsequent runs the bucket argument will be ignored.
registerNamespace :: Store s
                  => s
                  -> NameSpace
                  -> Word64
                  -- ^ Number of simple buckets to distribute over
                  -> Word64
                  -- ^ Number of extended buckets to distribute over
                  -> IO ()
registerNamespace s ns s_buckets e_buckets = do
    registered <- isRegistered s ns
    unless registered $
        append s ns [ ( name (undefined :: Tagged Simple Index)
                      , indexEntry 0 s_buckets)
                    , ( name (undefined :: Tagged Extended Index)
                      , indexEntry 0 e_buckets)
                    ]

-- | Write a mixed blob of points to the supplied store and namespace.
writeEncoded :: Store s => s -> NameSpace -> ByteString -> IO ()
writeEncoded s ns encoded =
    withLock s ns "write_lock" $ do
        -- Grab the latest index.
        (s_idx, e_idx) <- mustFetchIndexes s ns

        -- Collate points into epochs and buckets
        let (s_writes, e_writes, p_writes,
             s_latest, e_latest) = groupMixed s_idx e_idx encoded

        -- Merge our latest with everyone's latest.
        (s_latest', e_latest') <- updateLatest s ns s_latest e_latest

        e_offsets <- writeBuckets s ns s_writes e_writes p_writes

        -- Now check the sizes of those buckets so that we can decide if we
        -- want to trigger a rollover
        s_offsets <- unsafePartsOf (itraversed . withIndex)
                                   (getOffsets s ns SimpleBucketLocation)
                                   s_writes

        let threshold = rolloverThreshold s ns
        maybeRollover s ns threshold s_offsets s_latest' s_idx
        maybeRollover s ns threshold e_offsets e_latest' e_idx

-- | Do a roll over on the supplied index if any of the buckets have become too
-- large.
maybeRollover :: (Nameable (Tagged a Index), Store s)
              => s
              -> NameSpace
              -> Word64
              -> Map (Epoch,Bucket) Word64
              -> Tagged a Time
              -> Tagged a Index
              -> IO ()
maybeRollover s ns bucket_threshold offsets (Tagged (Time latest)) idx = do
    -- We only want to roll over for the latest epoch, otherwise we would roll
    -- over every time we wrote to an old (full) bucket.
    let (epoch, Bucket buckets) = indexLookup maxBound (untag idx)
    let wr_fld = maximumOf (ifolded . indices ((== epoch) . fst)) offsets
    case wr_fld of
        Just max_wr ->
            -- We compare the largest offset (from a write) with the threshold.
            when (max_wr > bucket_threshold)
                 (append s ns [(name idx, indexEntry latest buckets)])
        _ ->
            return ()

indexEntry :: Word64 -> Word64 -> ByteString
indexEntry epoch buckets =
    runPacking 16 (putWord64LE epoch >> putWord64LE buckets)

-- | The latest files ensure that we do not "cut off" any data when rolling
-- over to a new epoch (adding an entry to the index).
--
-- The epoch of the new entry in the index must be later than every point we
-- have seen up to now, or data would be lost.
updateLatest :: Store s => s
             -> NameSpace
             -> Tagged Simple Time
             -> Tagged Extended Time
             -> IO (Tagged Simple Time, Tagged Extended Time)
updateLatest s ns s_time e_time = withLock s ns "latest_update" $ do
    latests <- fetchs s ns [simpleLatest, extendedLatest]
    case latests & traversed . traversed %~ parse of
        [Just s_latest, Just e_latest] -> do
            write s ns $ maybeWrite s_latest s_time
                       ++ maybeWrite e_latest e_time
            return (max' s_latest s_time, max' e_latest e_time)
        [Nothing, Nothing] -> do
            -- First write
            write s ns [ (simpleLatest, pack . unTime . untag $ s_time)
                       , (extendedLatest, pack . unTime . untag $ e_time)
                       ]
            return (s_time, e_time)
        _ -> error "updateLatest: did not get both or no latest"
  where
    max' :: Word64 -> Tagged a Time -> Tagged a Time
    max' a (Tagged (Time b)) = Tagged . Time $ max a b

    -- | Only want to write if the new time is later than the existing one.
    maybeWrite :: forall a. Nameable (LatestFile a)
               => Word64 -> Tagged a Time -> [(ObjectName, ByteString)]
    maybeWrite latest (Tagged (Time t)) =
        [(name (LatestFile :: LatestFile a), pack t) | latest < t]

    pack x = runPacking 8 (putWord64LE x)
    parse = runUnpacking getWord64LE

    simpleLatest = name (LatestFile :: LatestFile Simple)
    extendedLatest = name (LatestFile :: LatestFile Extended)

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
    (Tagged s_ix, _) <- lift (mustFetchIndexes s ns)
    let objs = targetObjs s_ix start end addrs (name . SimpleBucketLocation)

    buffered readAhead (each objs >-> P.mapM (fetch s ns))
    >-> P.mapM (reifyFetch s)                  -- Load buckets into memory.
    >-> P.concat                               -- Just bs -> bs.
    >-> P.map (processSimple start end addrs)  -- Final processing.
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
    (_, Tagged e_ix) <- lift (mustFetchIndexes s ns)
    let s_objs = targetObjs e_ix start end addrs (name . SimpleBucketLocation)
    let e_objs = targetObjs e_ix start end addrs (name . ExtendedBucketLocation)

    -- There's probably a nicer abstraction for zipping two pipes together, we
    -- just use a tuple explicitly.
    let objs = zip s_objs e_objs

    buffered readAhead (each objs >-> P.mapM (both $ fetch s ns) )
    >-> P.mapM (both $ reifyFetch s) -- Load buckets into memory.
    >-> P.map sequenceT  -- Ensure both or neither are there.
    >-> P.concat         -- Maybe (s_bs,e_bs) -> (s_bs,e_bs.)
    >-> P.map (uncurry (processExtended start end addrs))
    >-> P.filter (not . S.null)
