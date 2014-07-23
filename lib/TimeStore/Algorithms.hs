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
{-# LANGUAGE NamedFieldPuns #-}

module TimeStore.Algorithms
(
    groupMixed,
    SimpleWrite(..),
    ExtendedWrite(..),
    PointerWrite(..),
) where

import Control.Applicative
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Data.ByteString.Builder (Builder, byteString, word64LE)
import qualified Data.Map as Map
import Data.Map.Strict (Map)
import Data.Monoid
import Data.Packer
import Data.Word (Word64)
import TimeStore.Core
import TimeStore.Index

-- | The data portion of extended points. For each entry into this write, there
-- will be a corresponding pointer from a SimpleWrite.
newtype ExtendedWrite = ExtendedWrite { unExtendedWrite :: Builder }
  deriving Monoid

-- | Intially any extended pointers within the simple portion of a write will
-- be pending a base offset for the extended counterparts. Once those offsets
-- are calculated, they may be fed in as a map to extract adjusted pointers.
data PointerWrite = PointerWrite Word64
                                 (Word64 -> Map (Object Extended) Word64 -> Builder)
instance Monoid PointerWrite where
    PointerWrite len1 f `mappend` PointerWrite len2 g =
        PointerWrite (len1 + len2) (\len0 oss -> f len0 oss <> g len1 oss)
    mempty = PointerWrite 0 (\_ _ -> mempty)
-- (PointerWrite 3 (\0 map -> "0") <> PointerWrite 5 (\3 map -> "3") <> PointerWrite 1 (\map -> "8") map 0

-- | A SimpleWrite is what we want to optimize for. Most writes will be simple.
newtype SimpleWrite = SimpleWrite { unSimpleWrite :: Builder }
  deriving Monoid

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
groupMixed :: Index Simple
           -> Index Extended
           -> ByteString
           -- Holy tuples batman!
           -> (Map (Object Simple) SimpleWrite,
               Map (Object Extended) ExtendedWrite,
               Map (Object Simple) PointerWrite,
               Time,
               Time)
groupMixed s_idx e_idx input = go mempty mempty mempty 0 0 0
  where
    go !s_map !e_map !p_map !s_latest !e_latest !os
        | fromIntegral os >= S.length input =
            (s_map, e_map, p_map, s_latest, e_latest)
        | otherwise = do
            let Point addr t len = parsePointAt os input
            let s_obj = indexLookup t addr s_idx
            let !s_latest' = if t > s_latest then t else s_latest

            if addr `testBit` 0
                -- This one is extended
                then do
                    let !e_latest' = if t > e_latest then t else e_latest
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
                    let e_obj = indexLookup t addr e_idx
                    let e_map' = Map.insertWith (flip mappend) e_obj e_wr e_map
                    -- This pointer will reference the extended write now.
                    let f = PointerWrite (len + 8) (makePointer e_obj addr t)
                    -- Insert pointer closure into the pointer map (grouped by
                    -- the simple index). This will be merged with the simple
                    -- writes once the offset is calculated and passed in.
                    let p_map' = Map.insertWith (flip mappend) s_obj f p_map'
                    go s_map e_map' p_map' s_latest' e_latest' (os + 24 + len)
                -- This one is simple
                else do
                    let s_wr = SimpleWrite (getBuilderAt os 24 input)
                    -- Shortcut the mappend here so that we don't need to
                    -- concatenate many thousands of mempties.
                    let s_map' = Map.insertWith' (flip mappend) s_obj s_wr s_map
                    go s_map' e_map p_map s_latest' e_latest (os + 24)

    makePointer :: Object Extended
                -> Address
                -> Time
                -> Word64
                -> Map (Object Extended) Word64
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

getBuilderAt :: Word64 -> Word64 -> ByteString -> Builder
getBuilderAt offset len bs = flip runUnpacking bs $ do
    unpackSetPosition (fromIntegral offset)
    byteString <$> getBytes (fromIntegral len)

