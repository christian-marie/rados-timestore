--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module TimeStore
(
    writeEncoded,
    MemoryStore,
    memoryStore,
) where

import Control.Lens hiding (Index, index, Simple)
import Data.ByteString (ByteString)
import Data.ByteString.Builder (toLazyByteString)
import Data.ByteString.Lazy (toStrict)
import Data.Tagged
import Data.Map.Strict (Map, unionWith)
import Control.Monad
import Data.Maybe
import Control.Applicative
import Data.Monoid
import Data.Word (Word64)
import Data.Packer
import TimeStore.Algorithms
import TimeStore.Core
import TimeStore.Index
import TimeStore.Stores.Memory

type Stream = ()

-- | Write a mixed blob of points to the supplied store and namespace.
writeEncoded:: Store s => s -> NameSpace -> Word64 -> ByteString -> IO ()
writeEncoded s ns bucket_threshold encoded =
    withLock s ns "write_lock" $ do
        -- Group our writes using the latest indexes
        (s_idx, e_idx) <- getIndexes s ns
        let (s_writes, e_writes, p_writes,
             s_latest, e_latest) = groupMixed s_idx e_idx encoded

        -- Merge our latest with everyone's latest.
        (s_latest', e_latest') <- updateLatest s ns s_latest e_latest

        -- We would like to batch up the request for offsets, so we convert a
        -- traversal over the indexes to a lens.
        --
        -- unsafePartsOf is needed so that we can change the return type, it
        -- will explode only if the length of domain to getOffsets is
        -- different to the length of the codomain.
        e_offsets <- unsafePartsOf (itraversed . withIndex) (getOffsets SimpleBucketLocation) e_writes

        -- Now adjust the offsets in the simple pointers.
        let p_adjusted = traversed %~ applyOffsets e_offsets $ p_writes
        let s_union = unionWith mappend s_writes p_adjusted

        -- Convert our ObjectGroups to ObjectNames for unioned simple objects.
        let s_objs = traversed %~ buildSimple $ itoList s_union

        -- Same for extended objects, also build the builders here.
        let e_objs = traversed %~ buildExtended $ itoList e_writes

        -- Stick all our writes into one big list (types are now unified) and
        -- do them at once.
        append s ns $ (s_objs, e_objs) ^.. both . traversed


        -- Now check the sizes of those buckets so that we can decide if we
        -- want to trigger a rollover
        s_offsets <- unsafePartsOf (itraversed . withIndex) (getOffsets ExtendedBucketLocation) s_union

        maybeRollover s ns bucket_threshold s_offsets s_latest' s_idx 
        maybeRollover s ns bucket_threshold e_offsets e_latest' e_idx
  where
    -- | Find the offsets of a batch of objects.
    --
    -- This must maintain the invariant that size of the domain equals the size
    -- of the codomain.
    getOffsets :: Nameable n => (a -> n) -> [(a, b)] -> IO [(a, Word64)]
    getOffsets naming_f xs = do
        let objs = map fst xs
        offsets <- sizes s ns (map (name . naming_f) objs)
        return $ zip objs (map (fromMaybe 0) offsets)

    applyOffsets :: Map (Epoch,Bucket) Word64 -> PointerWrite -> SimpleWrite
    applyOffsets offsets (PointerWrite _ f) = SimpleWrite (f 0 offsets)

    buildExtended :: ((Epoch,Bucket), ExtendedWrite) -> (ObjectName, ByteString)
    buildExtended = bimap (name . ExtendedBucketLocation)
                          (toStrict . toLazyByteString . unExtendedWrite)

    buildSimple :: ((Epoch, Bucket), SimpleWrite) -> (ObjectName, ByteString)
    buildSimple = bimap (name . SimpleBucketLocation)
                        (toStrict . toLazyByteString . unSimpleWrite)

-- | Do a roll over on the supplied index if any of the buckets have become too
-- large.
maybeRollover :: forall a s. (Nameable (Tagged a Index), Store s)
              => s
              -> NameSpace
              -> Word64
              -> Map (Epoch,Bucket) Word64
              -> Tagged a Time
              -> Tagged a Index
              -> IO ()
maybeRollover s ns bucket_threshold offsets (Tagged (Time latest)) idx = do
    let (epoch', Bucket buckets) = indexLookup maxBound (untag idx)
    let wr_fld = maximumOf (ifolded . indices ((== epoch') . fst)) offsets
    case wr_fld of
        Just max_wr ->
            when (max_wr > bucket_threshold)
                 (append s ns [(name idx, build buckets)])
        _ ->
            return ()
  where
    build buckets =
        runPacking 16 (putWord64LE latest >> putWord64LE buckets)

updateLatest :: Store s => s
             -> NameSpace
             -> Tagged Simple Time
             -> Tagged Extended Time
             -> IO (Tagged Simple Time, Tagged Extended Time)
updateLatest s ns s_time e_time = withLock s ns "latest_update" $ do
    latests <- fetchs s ns [simpleLatest, extendedLatest]
    let parsed = traversed . traversed %~ parse $ latests

    case parsed of
        [Just s_latest, Just e_latest] -> do
            write s ns $ mkWrite s_latest s_time 
                       ++ mkWrite e_latest e_time 
            return (max' s_latest s_time, max' e_latest e_time)
        [Nothing, Nothing] -> do
            -- First write
            write s ns $ [(simpleLatest, pack . unTime . untag $ s_time)
                         ,(extendedLatest, pack . unTime . untag $ e_time)]
            return (s_time, e_time)
        _ -> error "updateLatest: did not get both or no latest"
  where
    max' :: Word64 -> Tagged a Time -> Tagged a Time
    max' a (Tagged (Time b)) = Tagged . Time $ max a b 

    mkWrite :: forall a. Nameable (LatestFile a)
            => Word64 -> Tagged a Time -> [(ObjectName, ByteString)]
    mkWrite latest (Tagged (Time t)) =
        if latest < t
            then [(name (undefined :: LatestFile a), pack t)]
            else []

    pack x = runPacking 8 (putWord64LE x)
    parse = runUnpacking getWord64LE
    simpleLatest = "simple_latest"
    extendedLatest = "extended_latest"

getIndexes :: Store s => s -> NameSpace -> IO (Tagged Simple Index, Tagged Extended Index)
getIndexes s ns = do
    ixs <- fetchs s ns [name (undefined :: Tagged Simple Index)
                       ,name (undefined :: Tagged Extended Index)]
    case ixs of
        [Just s_idx, Just e_idx] ->
            return (Tagged (s_idx ^. index)
                   ,Tagged (e_idx ^. index))
        [Nothing, Nothing] ->
            error "Invalid origin" -- TODO: proper exception
        _ ->
            error "getIndexes: did not get all indexes or no indexes"

readAddrs :: Store s => s -> NameSpace -> [Address] -> IO Stream
readAddrs = undefined
