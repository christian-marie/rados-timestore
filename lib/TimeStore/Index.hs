--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

module TimeStore.Index
(
    Index(..),
    index,
    locationLookup,
    indexLookup,
) where

import Control.Lens (Iso', iso, itraverse_)
import Data.List
import qualified Data.Map as Map
import Data.Map(Map)
import Data.ByteString(ByteString)
import Data.Tagged
import Data.Packer
import Data.Monoid
import Data.Bits
import TimeStore.Core
import Control.Applicative

newtype Index = Index { unIndex :: Map Epoch Bucket }
    deriving (Monoid, Eq)

instance Nameable (Tagged Simple Index) where
    name _ = "simple_days"

instance Nameable (Tagged Extended Index) where
    name _ = "extended_days"

instance Show Index where
    show = intercalate "\n"
         . map (\(k,v) -> show k ++ "," ++ show v)
         . Map.toAscList
         . unIndex

index :: Iso' ByteString Index
index = iso byteStringToIndex indexToByteString
  where
    byteStringToIndex = 
        let parse = many $ (,) <$> (Epoch <$> getWord64LE)
                               <*> (Bucket <$> getWord64LE)
        in Index . Map.fromList . runUnpacking parse

    indexToByteString (Index m) =
        runPacking (Map.size m * 16) $
            flip itraverse_ m $ \(Epoch k) (Bucket v) ->
                putWord64LE k >> putWord64LE v

locationLookup :: Time -> Address -> Index -> (Epoch, Bucket)
locationLookup t (Address addr) ix = 
    let (epoch', Bucket max_bucket) = indexLookup t ix
        bucket' = Bucket $ (addr `clearBit` 0) `mod` max_bucket
    in (epoch', bucket')

indexLookup :: Time -> Index -> (Epoch, Bucket)
indexLookup t ix = fst (splitRemainder t ix)

-- Return first and the remainder that is later than that.
splitRemainder :: Time -> Index -> ((Epoch, Bucket), Index)
splitRemainder (Time t) (Index m) =
    let (left, middle, right) = Map.splitLookup (Epoch t) m
        first = case middle of
            Just n -> if Map.null left -- Corner case, leftmost entry
                        then (Epoch t, n)
                        else Map.findMax left
            Nothing -> Map.findMax left
    in (first, Index right)
