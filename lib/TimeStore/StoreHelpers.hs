--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

module TimeStore.StoreHelpers
(
    fetchIndexes,
    mustFetchIndexes,
    targetObjs,
    readAhead,
    buffered,
    writeBuckets,
    getOffsets,
) where


import Control.Exception
import Control.Lens hiding (Index, Simple, each, index)
import Data.ByteString (ByteString)
import Data.ByteString.Builder (toLazyByteString)
import Data.ByteString.Lazy (toStrict)
import Data.List (nub)
import Data.Map.Strict (Map, unionWith)
import Data.Maybe
import Data.Monoid
import Data.Tagged
import Data.Word
import Pipes
import TimeStore.Algorithms
import TimeStore.Core
import TimeStore.Index

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

mustFetchIndexes :: Store s
                 => s
                 -> NameSpace
                 -> IO (Tagged Simple Index, Tagged Extended Index)
mustFetchIndexes s ns =
    fetchIndexes s ns >>= \x -> case x of
        Just y  -> return y
        Nothing -> throwIO (userError "Invalid namespace")

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
        nub [(epoch, placeBucket max_buckets a) | a <- addrs ] ++ acc


writeBuckets :: Store s
             => s
             -> NameSpace
             -> Map (Epoch, Bucket) SimpleWrite
             -> Map (Epoch, Bucket) ExtendedWrite
             -> Map (Epoch, Bucket) PointerWrite
             -> IO (Map (Epoch, Bucket) Word64)
writeBuckets s ns s_writes e_writes p_writes = do
        -- We would like to batch up the request for offsets, so we convert a
        -- traversal over the indexes to a lens.
        --
        -- unsafePartsOf is needed so that we can change the return type, it
        -- will explode only if the length of domain to getOffsets is
        -- different to the length of the codomain.
        e_offsets <- unsafePartsOf (itraversed . withIndex)
                                   (getOffsets s ns ExtendedBucketLocation)
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
        return e_offsets
  where
    applyOffsets :: Map (Epoch,Bucket) Word64 -> PointerWrite -> SimpleWrite
    applyOffsets offsets (PointerWrite _ f) = SimpleWrite (f 0 offsets)

    buildExtended :: ((Epoch,Bucket), ExtendedWrite) -> (ObjectName, ByteString)
    buildExtended = bimap (name . ExtendedBucketLocation)
                          (toStrict . toLazyByteString . unExtendedWrite)

    buildSimple :: ((Epoch, Bucket), SimpleWrite) -> (ObjectName, ByteString)
    buildSimple = bimap (name . SimpleBucketLocation)
                        (toStrict . toLazyByteString . unSimpleWrite)

-- | Find the offsets of a batch of objects.
--
-- This must maintain the invariant that size of the domain equals the size
-- of the codomain.
getOffsets :: (Store s, Nameable n)
           => s
           -> NameSpace
           -> (a -> n)
           -> [(a, b)]
           -> IO [(a, Word64)]
getOffsets s ns naming_f xs = do
    let objs = map fst xs
    offsets <- sizes s ns (map (name . naming_f) objs)
    return $ zip objs (map (fromMaybe 0) offsets)

-- How many buckets to fetch-ahead, we will want roughly this much memory *
-- average block size.
--
-- This is a trade-off between memory use and latency for large sequential
-- reads.
--
-- If each bucket is rougly 4MB, then this read ahead level (16) will try to
-- read 4MB * 16 = 64MB ahead.
readAhead :: Int
readAhead = 16

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


