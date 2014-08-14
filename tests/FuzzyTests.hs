--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedLists            #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

import Test.Hspec.QuickCheck
import Test.QuickCheck
import Control.Applicative
import Control.Lens hiding (Index, Simple, index)
import Control.Monad
import Data.Bits.Lens
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Data.ByteString.Builder
import qualified Data.ByteString.Lazy as L
import qualified Data.Map as Map
import Data.Map.Strict (Map)
import Data.Monoid
import Data.String
import Data.Tagged
import Data.Vector.Storable (Vector)
import Data.Vector.Storable.ByteString
import Data.Word (Word64)
import Pipes (Producer)
import qualified Pipes.Prelude as Pipes
import Test.Hspec
import TimeStore
import TimeStore.Algorithms
import TimeStore.Core
import TimeStore.Index
import qualified TimeStore.Mutable as MutableTS


newtype MixedPayload = MixedPayload { unMixedPayload :: ByteString }
  deriving (Eq, Show)

newtype ExtendedPoint = ExtendedPoint { unExtendedPoint :: ByteString }
  deriving (Eq, Show)

newtype SimplePoint = SimplePoint { unSimplePoint :: ByteString }

instance Arbitrary MixedPayload where
    arbitrary = do
        len <- (`mod` 1024) . abs <$> arbitrary
        MixedPayload . L.toStrict . L.fromChunks <$> go [] len
      where
        go :: [ByteString] -> Int -> Gen [ByteString]
        go xs 0 = return xs
        go xs n = do
            p <- arbitrary
            case p of
                Left  (ExtendedPoint x) -> go (x:xs) (pred n)
                Right (SimplePoint x)   -> go (x:xs) (pred n)

deriving instance Arbitrary Address
deriving instance Arbitrary Time
deriving instance Arbitrary Epoch
deriving instance Arbitrary Bucket

instance Arbitrary Point where
    arbitrary =
        Point <$> arbitrary <*> arbitrary <*> arbitrary

instance Arbitrary ExtendedPoint where
    arbitrary = do
        p <- arbitrary
        let p'@(Point _ _ len) = p & address . bitAt 0 .~ True
                                   & payload .&.~ 0xff

        -- This is kind of slow
        pl <- S.pack <$> vectorOf (fromIntegral len) arbitrary
        return . ExtendedPoint $ vectorToByteString [p'] <> pl

instance Arbitrary SimplePoint where
    arbitrary = do
        p <- liftA (address . bitAt 0 .~ False) arbitrary
        return . SimplePoint $ vectorToByteString [p]

instance Arbitrary Index where
    arbitrary = do
        -- 12 seems a little low to cause slow-down, need to look in to why.
        n <- (`mod` 8) <$> arbitrary 
        (Positive first) <- arbitrary
        xs <- go n 0
        return . Index . Map.fromList $ (0, first) : xs
      where
        go :: Int -> Epoch -> Gen [(Epoch, Bucket)]
        go 0 _ = return []
        go n prev = do
            (Positive buckets) <- arbitrary
            epoch <- arbitrary `suchThat` (> prev)
            next <- go (pred n) epoch
            return $ (epoch, buckets) : next

main :: IO ()
main = hspec $
    prop "Groups ponts" propGroupsPoints

propGroupsPoints :: Index -> Index -> MixedPayload -> Bool
propGroupsPoints ix1 ix2 (MixedPayload x) =
    let (_, _, _,
         Tagged s_max, Tagged e_max) = groupMixed (Tagged ix1)  (Tagged ix2) x
    -- There is only one invariant I can think of given no knowledge of
    -- incoming data. The simple max should be less than or equal to the
    -- extended max. This is because adding an extended point will add a
    -- pointer to the simple bucket.
    in e_max <= s_max
