--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module TimeStore
(
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
import Control.Exception
import Control.Lens hiding (Index, Simple, each, index)
import Control.Monad
import Data.ByteString (ByteString)
import Data.ByteString.Builder (toLazyByteString)
import Data.ByteString.Lazy (toStrict)
import Data.List (nub)
import Data.Map.Strict (Map, unionWith)
import Data.Maybe
import Data.Monoid
import Data.Packer
import Data.Tagged
import Data.Word (Word64)
import Pipes
import qualified Pipes.Prelude as P
import TimeStore.Algorithms
import TimeStore.Core
import TimeStore.Index
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
writeEncoded :: Store s => s -> NameSpace -> Word64 -> ByteString -> IO ()
writeEncoded s ns bucket_threshold encoded =
    withLock s ns "write_lock" $ do
        -- Grab the latest index.
        (s_idx, e_idx) <- mustFetchIndexes s ns

        -- Collate points into epochs and buckets
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
        e_offsets <- unsafePartsOf (itraversed . withIndex)
                                   (getOffsets ExtendedBucketLocation)
                                   e_writes

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
        s_offsets <- unsafePartsOf (itraversed . withIndex)
                                   (getOffsets SimpleBucketLocation)
                                   s_union

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

-- | Attempt to fetch and parse the simple and extended indexes from the data
-- store.
fetchIndexes :: Store s
             => s -> NameSpace
             -> IO (Maybe (Tagged Simple Index, Tagged Extended Index))
fetchIndexes s ns = do
    ixs <- fetchs s ns [name (undefined :: Tagged Simple Index)
                       ,name (undefined :: Tagged Extended Index)]
    return $ case sequence ixs of
        Just [s_idx, e_idx] ->
            Just ( Tagged (s_idx ^. index)
                 , Tagged (e_idx ^. index)
                 )
        Nothing -> Nothing
        _ ->
            error "getIndexes: impossible"

-- | Fetch the index, throw an error if it isn't ther.
mustFetchIndexes :: Store s
                 => s
                 -> NameSpace
                 -> IO (Tagged Simple Index, Tagged Extended Index)
mustFetchIndexes s ns =
    fetchIndexes s ns >>= \x -> case x of
        Just y  -> return y
        Nothing -> throwIO (userError "Invalid namespace")


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
    readAhead s ns objs >-> P.map (processSimple start end addrs)

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
    let objs = targetObjs e_ix start end addrs (name . ExtendedBucketLocation)
    readAhead s ns objs

-- | Find the target object names for all given addresses within the range,
-- indexed by the provided index.
targetObjs :: Index
           -> Time
           -> Time
           -> [Address]
           -> ((Epoch, Bucket) -> ObjectName)
           -> [ObjectName]
targetObjs idx start end addrs name_f  =
    -- We look up the Epoch and number of Buckets within the time range.
    let range = rangeLookup start end idx

    -- Given a range of Epoch to search through, we want to grap all of the
    -- (Epoch, Bucket) tuples that which correspond to our addresses, which
    -- will give us the destination bucket names.
    --
    -- Achieving this is almost trivial, we just need to map mod over our
    -- addresses with the number of buckets in each epoch taken into account.
    -- We use the unique image of this function to grab all the buckets needed,
    -- without having to request one bucket for each address individually.
        targets = foldr hashBuckets [] range
     in
        map name_f targets

  where
    hashBuckets (epoch, max_buckets) acc =
        nub [(epoch, simpleBucket max_buckets a) | a <- addrs ] ++ acc

-- | Stream the provided objects out, whilst fetching a few ahead of time.
-- Empty buckets are ignored.
--
-- This is a trade-off between memory use and latency for large sequential
-- reads.
--
-- If each bucket is rougly 4MB, then this read ahead level (16) will try to
-- read 4MB * 16 = 64MB ahead.
readAhead :: Store s
          => s
          -> NameSpace
          -> [ObjectName]
          -> Producer ByteString IO ()
readAhead s ns objs = 
    buffered 16 (each objs >-> P.mapM (fetch s ns)) -- Request buckets
    >-> P.mapM (reifyFetch s)                       -- Load them
    >-> P.concat                                    -- Just bs -> bs

-- | Fill up a buffer before yielding any values, the buffer will be kept full
-- until the underlying producer is done.
buffered :: Monad m => Int -> Producer a m r -> Producer a m r
buffered n = go []
  where
    go as prod =
        if length as >= n
            then do
                yield (last as)
                go (init as) prod
            else do
                x <- lift (next prod)
                case x of
                    Left r -> do
                        mapM_ yield (reverse as)
                        return r
                    Right (a, rest) ->
                        go (a:as) rest


