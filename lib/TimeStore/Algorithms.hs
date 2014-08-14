--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}

module TimeStore.Algorithms
(
    groupMixed,
    SimpleWrite(..),
    ExtendedWrite(..),
    PointerWrite(..),
    processSimple,
    processExtended,
    processMutable,
) where

import Control.Applicative
import Control.Monad
import Control.Monad.Primitive (PrimMonad, PrimState)
import Control.Monad.ST (runST)
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Data.ByteString.Builder (Builder, byteString, toLazyByteString,
                                word64LE)
import qualified Data.ByteString.Lazy.Char8 as L
import Data.Function (on)
import qualified Data.Map as Map
import Data.Map.Strict (Map)
import Data.Monoid
import Data.Packer
import Data.String (IsString)
import Data.Tagged
import Data.Vector.Generic.Mutable (MVector)
import qualified Data.Vector.Generic.Mutable as M
import qualified Data.Vector.Storable as V
import Data.Vector.Storable.ByteString
import Data.Word (Word64)
import Hexdump
import TimeStore.Core
import TimeStore.Index

import qualified Data.Vector.Algorithms.Merge as Merge

-- | The data portion of extended points. For each entry into this write, there
-- will be a corresponding pointer from a SimpleWrite.
newtype ExtendedWrite = ExtendedWrite { unExtendedWrite :: Builder }
  deriving (Monoid, IsString)

instance Show ExtendedWrite where
    show =
        ("ExtendedWrite: "++ ) . prettyHex . L.toStrict . toLazyByteString . unExtendedWrite

instance Eq ExtendedWrite where
    (ExtendedWrite a) == (ExtendedWrite b) =
        toLazyByteString a == toLazyByteString b

-- | Intially any extended pointers within the simple portion of a write will
-- be pending a base offset for the extended counterparts. Once those offsets
-- are calculated, they may be fed in as a map to extract adjusted pointers.
data PointerWrite = PointerWrite Word64
                                 (Word64 -> Map (Epoch, Bucket) Word64 -> Builder)

instance Monoid PointerWrite where
    PointerWrite len1 f `mappend` PointerWrite len2 g =
        PointerWrite (len1 + len2) (\len0 oss -> f len0 oss <> g len1 oss)
    mempty = PointerWrite 0 (\_ _ -> mempty)

-- We can only show the current offset, as the pointers are waiting for an
-- offset map, which we don't have yet.
instance Show PointerWrite where
    show (PointerWrite os _) =
        "PointerWrite (current offset: " ++ show os ++ ")"

instance Eq PointerWrite where
    (PointerWrite os _) == (PointerWrite os' _) =
        os == os'

-- | A SimpleWrite is what we want to optimize for. Most writes will be simple.
newtype SimpleWrite = SimpleWrite { unSimpleWrite :: Builder }
  deriving (Monoid, IsString)

instance Show SimpleWrite where
    show =
        ("SimpleWrite: " ++) . prettyHex . L.toStrict . toLazyByteString . unSimpleWrite

instance Eq SimpleWrite where
    (SimpleWrite a) == (SimpleWrite b) =
        toLazyByteString a == toLazyByteString b

-- | Take a blob of mixed (simple and extended) points and split them into two
-- parts, indexed by the provided index. We also return the latest time seen
-- (maximum of time over the respective datasets).
--
-- The first part is a Vector of 24 byte points, these are either complete
-- points in of themselves or 'pointers' to extended records. The point types
-- are differentiated by the last bit of the address.
--
-- This function probably does too much, but it is a trade-off between smaller
-- functions and having to tag the extended bytes with times so that they can
-- be indexed later.
groupMixed :: Tagged Simple Index
           -> Tagged Extended Index
           -> ByteString
           -- Holy tuples batman!
           -> (Map (Epoch,Bucket) SimpleWrite,
               Map (Epoch,Bucket) ExtendedWrite,
               Map (Epoch,Bucket) PointerWrite,
               Tagged Simple Time,
               Tagged Extended Time)
groupMixed (Tagged s_idx) (Tagged e_idx) input = go mempty mempty mempty (Tagged 0) (Tagged 0) 0
  where
    -- Look through the input string, indexed by os.
    go !s_map !e_map !p_map !s_latest !e_latest !os
        | fromIntegral os >= S.length input =
            (s_map, e_map, p_map, s_latest, e_latest)
        | otherwise = do
            let Point addr t len = parsePointAt os input
            let !s_loc = locationLookup t addr s_idx
            let !s_latest' = if t > untag s_latest then Tagged t else s_latest

            if addr `testBit` 0
                -- This one is extended
                then do
                    let !e_latest' = if t > untag e_latest then Tagged t else e_latest
                    -- Our extended data is just after the Point, the length of
                    -- which is described in the point.
                    --
                    -- The length test is due to this:
                    -- https://github.com/vincenthz/hs-packer/pull/8
                    --
                    -- When a newer version is released, the check can be
                    -- removed.
                    let e_bytes = if len == 0
                                    then mempty
                                    else getBuilderAt (os + 24) len input

                    let e_wr = ExtendedWrite (word64LE len <> e_bytes)
                    let e_loc = locationLookup t addr e_idx

                    let e_map' =  Map.insertWith (flip mappend) e_loc e_wr e_map
                    -- This pointer will reference the extended write now.
                    let f = PointerWrite (len + 8) (makePointer e_loc addr t)
                    -- Insert pointer closure into the pointer map (grouped by
                    -- the simple index). This will be merged with the simple
                    -- writes once the offset is calculated and passed in.
                    let p_map' = Map.insertWith (flip mappend) s_loc f p_map
                    go s_map e_map' p_map' s_latest' e_latest' (os + 24 + len)
                -- This one is simple
                else do
                    let s_wr = SimpleWrite (getBuilderAt os 24 input)
                    -- Shortcut the mappend here so that we don't need to
                    -- concatenate many thousands of mempties.
                    let s_map' = Map.insertWith (flip mappend) s_loc s_wr s_map
                    go s_map' e_map p_map s_latest' e_latest (os + 24)

    makePointer :: (Epoch,Bucket)
                -> Address
                -> Time
                -> Word64
                -> Map (Epoch,Bucket) Word64
                -> Builder
    makePointer obj (Address addr) (Time time') base_os os_map =
        case Map.lookup obj os_map of
            Nothing -> error "makePointer: bug: no offset for extended pointer"
            Just os -> word64LE addr <> word64LE time' <> word64LE (os + base_os)

-- | Read a point from an offset into a ByteString.
parsePointAt :: Word64 -> ByteString -> Point
parsePointAt os bs = flip runUnpacking bs $ do
    unpackSetPosition (fromIntegral os)
    Point <$> (Address <$> getWord64LE)
          <*> (Time <$> getWord64LE)
          <*> getWord64LE

-- | Read from the given offset len bytes, returning a builder of those bytes.
getBuilderAt :: Word64 -> Word64 -> ByteString -> Builder
getBuilderAt offset len bs = flip runUnpacking bs $ do
    unpackSetPosition (fromIntegral offset)
    byteString <$> getBytes (fromIntegral len)


-- | Filter out points not within with the given criteria, then sort and
-- de-duplicate.
--
-- O(n lg n)
processSimple :: Time
              -> Time
              -> [Address]
              -> ByteString
              -> ByteString
processSimple start end addr input = runST $ do
    let v = V.filter (\(Point a t _) -> elem a addr
                                     && t >= start
                                     && t <= end)
          . byteStringToVector $ input
    mv <- V.thaw v
    Merge.sort mv
    vectorToByteString <$> (deDuplicate similar mv >>= V.freeze)
  where
    -- | Is the address and time the same? We don't want to know about the
    -- payload for de-duplication.
    similar :: Point -> Point -> Bool
    similar (Point a t _) (Point a' t' _) =
        a == a' && t == t'

-- | Given a simple and extended bucket, do the usual processing on the simple
-- bucket, then grab the extended portions and merge the two together.
processExtended :: Time
                -> Time
                -> [Address]
                -> ByteString
                -> ByteString
                -> ByteString
processExtended start end addrs s_bs e_bs =
    mergeExtended e_bs $ processSimple start end addrs s_bs

mergeExtended :: ByteString -> ByteString -> ByteString
mergeExtended e_bs =
    L.toStrict
    . toLazyByteString
    . V.foldl' merge mempty
    . byteStringToVector
  where
    merge acc (Point (Address a) (Time t) os) =
        let bytes = runUnpacking (getExtendedBytes os) e_bs
            bldr = word64LE a <> word64LE t <> byteString bytes
        in acc <> bldr

-- | This is a special case, where we simply want to find the latest of each
-- unique address.
processMutable :: ByteString -> ByteString -> ByteString
processMutable s_bs e_bs = mergeExtended e_bs $ latestUniques s_bs

-- | Extract the latest (by time) of each unique address. This is done by
-- sorting address ascending and time descending, then de-duplicating by
-- address. Thus, the "first write wins" behaviour produces the last element of
-- the sequence.
latestUniques :: ByteString -> ByteString
latestUniques bs = runST $ do
    mv <- V.thaw (byteStringToVector bs)
    Merge.sortBy cmp mv
    vectorToByteString <$> (deDuplicate ((==) `on` _address) mv >>= V.freeze)
  where
    -- Compare addresses first, then time "in reverse", to place the latest
    -- time at the beginning of the sequence.
    cmp :: Point -> Point -> Ordering
    cmp (Point a t _)  (Point a' t' _) =
        case a `compare` a' of
            EQ -> t' `compare` t
            x  -> x

-- First word is the length, then the string. We return the length and the
-- string as a string.
getExtendedBytes :: Word64 -> Unpacking ByteString
getExtendedBytes offset = do
    unpackSetPosition (fromIntegral offset)
    len <- getWord64LE
    unpackSetPosition (fromIntegral offset)
    getBytes (fromIntegral len + 8)

-- | Sort and de-duplicate elements. First element wins.
deDuplicate :: (PrimMonad m, MVector v e, Ord e)
            => (e -> e -> Bool)
            -> v (PrimState m) e
            -> m (v (PrimState m) e)
deDuplicate cmp input
    | M.null input = return input
    | otherwise = do
        first <- M.unsafeRead input 0
        go input first 1 1 (M.length input)
  where
    go buf prev_elt read_ptr write_ptr len
        | read_ptr == len = return $ M.take write_ptr buf
        | otherwise = do
            elt <- M.unsafeRead buf read_ptr

            if elt `cmp` prev_elt
                then
                    go buf prev_elt (succ read_ptr) write_ptr len
                else do
                    -- This conditional is an optimization for non-duplicate
                    -- data.
                    when (write_ptr /= read_ptr) $
                        M.unsafeWrite buf write_ptr elt

                    go buf elt (succ read_ptr) (succ write_ptr) len
