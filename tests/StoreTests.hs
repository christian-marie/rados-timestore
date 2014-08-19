--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

import Control.Applicative
import Control.Concurrent.Async
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Function
import Data.List
import Data.Monoid
import Test.Hspec
import Test.Hspec.Core (SpecM)
import Test.Hspec.QuickCheck
import Test.QuickCheck hiding (reason, theException)
import Test.QuickCheck.Monadic
import TimeStore
import TimeStore.Core

newtype ValidBS
    = ValidBS { unValidBS :: ByteString }

instance Arbitrary ValidBS where
    arbitrary =
        ValidBS . S.pack <$> (listOf1 . elements $ ['A'..'Z'] ++ ['a'..'z'] ++ ['0'..'9'])

instance Arbitrary NameSpace where
    arbitrary = do
        ns <- nameSpace . unValidBS <$> arbitrary
        case ns of
            Left _ -> arbitrary
            Right x -> return x

instance Arbitrary ObjectName where
    arbitrary =
        ObjectName . unValidBS <$> arbitrary

instance Arbitrary ByteString where
    arbitrary =
        S.pack <$> arbitrary

main :: IO ()
main =
    hspec $
        describe "memory store" $ testStore (memoryStore 64)

testStore :: Store s => IO s -> SpecM ()
testStore store =  do
            prop "appending list is same as mconcat" $
                propAppendConcat store
            prop "overwriting writes after appending subset is writes" $
                propAppendWrite store
            prop "sizes are sizes" $
                propSizes store
            it "has a well behaved lock" $
                lockTest store

testNS :: NameSpace
testNS = NameSpace "PONIES"

lockTest :: forall s. Store s => IO s -> Expectation
lockTest store = do
    s <- store
    writeCtr s 0

    as <- replicateM 100 . async . withLock s testNS "a_lock" $ do
        n <- readCtr s
        writeCtr s (succ n)
    mapM_ wait as

    readCtr s >>= (`shouldBe` 100)
  where
    ctr :: ObjectName
    ctr = "counter"

    writeCtr :: s -> Int -> IO ()
    writeCtr s n = write s testNS [(ctr, S.pack . show $ n)]

    readCtr :: s -> IO Int
    readCtr s = do
        [Just n] <- fetchs s testNS [ctr]
        return . read . S.unpack $ n

propSizes :: Store s
          => IO s
          -> NameSpace
          -> NonEmptyList (ObjectName, ByteString)
          -> Property
propSizes store ns (NonEmpty writes) =
    monadicIO $ do
        let unique_writes = nubBy ((==) `on` fst) writes
        xs <- run $ do
            s <- store
            write s ns unique_writes
            sizes s ns (map fst unique_writes)
        let expected = map (Just . fromIntegral . S.length . snd) unique_writes
        assert (xs == expected)

propAppendConcat :: Store s
                 => IO s
                 -> NameSpace
                 -> ObjectName
                 -> NonEmptyList ByteString
                 -> Property
propAppendConcat store ns obj (NonEmpty bss) =
    monadicIO $ do
        xs <- run $ do
            s <- store
            forM_ bss $ \bs ->
                append s ns [(obj, bs)]
            fetchs s ns [obj]
        assert (xs == [Just (mconcat bss)])

-- | This will also test the ordering of duplicate writes.
propAppendWrite :: Store s
                 => IO s
                 -> NameSpace
                 -> NonEmptyList (ObjectName, ByteString)
                 -> Positive Int
                 -> Property
propAppendWrite store ns (NonEmpty writes) (Positive rand) =
    monadicIO $ do
        -- We want only the last writes to each object
        let unique_writes = reverse . nubBy ((==) `on` fst) . reverse $ writes
        xs <- run $ do
            s <- store
            let appends = take (length writes `mod` rand) unique_writes
            append s ns appends
            write s ns writes
            fetchs s ns (map fst unique_writes)
        assert (xs == map (Just . snd) unique_writes)
