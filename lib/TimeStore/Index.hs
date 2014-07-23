--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module TimeStore.Index
(
    Index(..),
    index,
    indexLookup,
) where

import Control.Lens (Iso', iso, itraverse_)
import Data.List
import qualified Data.Map as Map
import Data.Map(Map)
import Data.ByteString(ByteString)
import Data.Packer
import Data.Monoid
import Data.Bits
import TimeStore.Core
import Control.Applicative

newtype Index kind = Index { unIndex :: Map Epoch Bucket }
    deriving (Monoid, Eq)

instance Show (Index a) where
    show = intercalate "\n"
         . map (\(k,v) -> show k ++ "," ++ show v)
         . Map.toAscList
         . unIndex

index :: Iso' ByteString (Index a)
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

indexLookup :: Time -> Address -> Index a -> Object a
indexLookup t (Address addr) ix = 
    let ((epoch', Bucket max_bucket),_) = splitRemainder t ix
        bucket' = Bucket $ (addr `clearBit` 0) `mod` max_bucket
    in Object epoch' bucket'

-- Return first and the remainder that is later than that.
splitRemainder :: Time -> Index a -> ((Epoch, Bucket), Index a)
splitRemainder (Time t) (Index m) =
    let (left, middle, right) = Map.splitLookup (Epoch t) m
        first = case middle of
            Just n -> if Map.null left -- Corner case, leftmost entry
                        then (Epoch t, n)
                        else Map.findMax left
            Nothing -> Map.findMax left
    in (first, Index right)

lookupRange :: Time -> Time -> Index a -> [(Epoch, Bucket)]
lookupRange start (Time end) dm =
    let (first, (Index remainder)) = splitRemainder start dm
        (rest,_) = Map.split (Epoch end) remainder
    in first : Map.toList rest
