--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedLists            #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

import Control.Applicative
import Control.Lens hiding (Index, Simple, index)
import Control.Monad
import Data.Bits.Lens
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy as L
import Data.Function
import Data.List
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Monoid
import Data.Tagged
import Data.Vector.Storable.ByteString
import Data.Word (Word64)
import qualified Pipes.Prelude as P
import Test.Hspec
import Test.Hspec.QuickCheck
import Test.QuickCheck hiding (reason, theException)
import Test.QuickCheck.Monadic
import TimeStore
import TimeStore.Algorithms
import TimeStore.Core
import TimeStore.Index
import qualified TimeStore.Mutable as Mutable

newtype MixedPayload
  = MixedPayload { unMixedPayload :: ByteString }
  deriving (Eq, Show)

newtype ExtendedPoint
  = ExtendedPoint { unExtendedPoint :: ByteString }
  deriving (Eq, Show)

newtype SimplePoint
  = SimplePoint { unSimplePoint :: ByteString }
  deriving (Eq, Show)

instance Arbitrary MixedPayload where
    arbitrary = do
        len <- (\(Positive n) ->  n `mod` 1048576) <$> arbitrary
        MixedPayload . L.toStrict . L.fromChunks <$> go len
      where
        go :: Int -> Gen [ByteString]
        go 0 = return []
        go n = do
            p <- arbitrary
            case p of
                Left  (ExtendedPoint x) -> (x:) <$> go (pred n)
                Right (SimplePoint x)   -> (x:) <$> go (pred n)

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
        (Positive first) <- arbitrary
        xs <- sortBy (compare `on` fst)
              . map (\(Positive x, Positive y) -> (x, (y `mod` 128) + 1))
              <$> arbitrary
        return . Index . Map.fromList $ (0, first) : xs


-- A helper for chaining future propositions awaiting an input i.
data Proposition i
    = Proposition { unProposition :: i -> Either String () }
instance Show (Proposition i) where
    show _ = "Proposition"

instance Monoid (Proposition i) where
  mempty = Proposition (const (Right ()))
  Proposition a `mappend` Proposition b =
    Proposition (\x -> a x >> b x)

propose :: String -> (i -> Bool) -> Proposition i
propose tag f = Proposition (\i -> if f i then Right () else Left tag)

main :: IO ()
main = hspec $ do
    prop "Point grouping" propGroupsPoints
    prop "Mutable store" propMutableStore
    prop "Immutable store" propImmutableStore

data MutableWrites
  = MutableWrites [(Address, ByteString)] (Proposition (Map Address ByteString))
  deriving Show

instance Arbitrary MutableWrites where
    arbitrary = do
        len <- (\(Positive n) ->  n `mod` 1048576) <$> arbitrary
        (prop_map, entries) <- go len

        let props = mconcat $ Map.elems prop_map

        return $ MutableWrites entries props
      where
        go :: Int
           -> Gen (Map Address WriteProp, [(Address, ByteString)])
        go 0 = return (mempty, mempty)
        go n = do
            (props, xs) <- go (pred n)
            addr <- (bitAt 0 .~ True) <$> arbitrary
            bytes <- S.pack <$> arbitrary
            let p = propose ("lookup " ++ show addr)
                            (\rds -> case Map.lookup addr rds of
                                        Nothing -> False
                                        Just bs -> bs == bytes)
            let props' = Map.insertWith (flip const) addr p props
            return (props', (addr, bytes):xs)


data ImmutableWrites
  = ImmutableWrites [(Address, ByteString)] (Proposition (Map Address ByteString))
  deriving Show

type WriteProp = Proposition (Map Address ByteString)

instance Arbitrary ImmutableWrites where
    arbitrary = do
        len <- (\(Positive n) ->  n `mod` 1048576) <$> arbitrary
        (prop_map, entries) <- go len

        let props = mconcat $ Map.elems prop_map

        return $ ImmutableWrites entries props
      where
        go :: Int
           -> Gen (Map Address WriteProp, [(Address, ByteString)])
        go 0 = return (mempty, mempty)
        go n = do
            (props, xs) <- go (pred n)
            p <- arbitrary

            (proposition, bs) <- if p ^. address . bitAt 0
                                    then doExtended p
                                    else doSimple p

            let props' = Map.insert (p ^. address) proposition props
            return (props', (p ^. address, bs):xs)

        doExtended p = do
            let p' = p & payload %~ (`mod` 1024)
            pl <- S.pack <$> vectorOf (fromIntegral $ p' ^. payload) arbitrary
            let proposition = propose ("lookup extended " ++ p' ^. address . to show)
                                      (\rds -> case Map.lookup (p' ^. address) rds of
                                                Nothing ->
                                                    False
                                                Just bs ->
                                                    not . S.null . snd $ pl `S.breakSubstring` bs)
            return (proposition, vectorToByteString [p'] <> pl)

        doSimple p = do
            let bs  = vectorToByteString [p]
            let proposition = propose ("lookup simple " ++ p ^. address . to show)
                                      (\rds -> case Map.lookup (p ^. address) rds of
                                                Nothing ->
                                                    False
                                                Just bs' ->
                                                    not . S.null . snd $ bs `S.breakSubstring` bs')
            return (proposition, bs)

testNS :: NameSpace
testNS = "PONIES"

propMutableStore :: MutableWrites -> Property
propMutableStore (MutableWrites writes p) = monadicIO $ do
    res <- run $ do
        s <- memoryStore 42
        mapM_ (uncurry (Mutable.insert s testNS)) writes
        foldM (lookupAndInsert s) mempty (map fst writes)
    assert . either error (const True) $ unProposition p res
  where
     lookupAndInsert s acc addr = do
        maybe_bs <- Mutable.lookup s testNS addr
        case maybe_bs of
            Nothing -> error $ "Lost data at " ++ show addr
            Just bs -> return $ Map.insert addr bs acc

propImmutableStore :: ImmutableWrites -> Positive Word64 -> Positive Word64 -> Property
propImmutableStore (ImmutableWrites writes p)
                   (Positive s_buckets)
                   (Positive e_buckets) = monadicIO $ do
    res <- run $ do
        s <- memoryStore 42
        registerNamespace s testNS s_buckets e_buckets
        mapM_ (writeEncoded s testNS . snd) writes
        foldM (lookupAndInsert s) mempty (map fst writes)
    assert . either error (const True) $ unProposition p res
  where
     lookupAndInsert s acc addr = do
        let prod = if addr ^. bitAt 0
            then readExtended s testNS 0 maxBound [addr]
            else readSimple s testNS 0 maxBound  [addr]
        maybe_bs <- P.head prod
        case maybe_bs of
            Nothing -> error $ "Lost data at " ++ show addr
            Just bs -> return $ Map.insert addr bs acc

propGroupsPoints :: Index -> Index -> MixedPayload -> Bool
propGroupsPoints ix1 ix2 (MixedPayload x) =
    let (_, _, _,
         Tagged s_max, Tagged e_max) = groupMixed (Tagged ix1)  (Tagged ix2) x
    -- There is only one invariant I can think of given no knowledge of
    -- incoming data. The simple max should be less than or equal to the
    -- extended max. This is because adding an extended point will add a
    -- pointer to the simple bucket.
    in e_max <= s_max

