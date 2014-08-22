--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

import Control.Applicative
import Control.Concurrent (threadDelay)
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

type With x = forall a. (x -> IO a) -> IO a

main :: IO ()
main =
    hspec $ do
        describe "memory store" $
            testStore (memoryStore 64 >>=)
        describe "rados store" $
            testStore (withRadosStore Nothing "/etc/ceph/ceph.conf" "test" 64)

testStore :: Store s => With s -> SpecM ()
testStore ws =  do
     prop "appending list is same as mconcat" $
         propAppendConcat ws
     prop "overwriting writes after appending subset is writes" $
         propAppendWrite ws
     prop "sizes are sizes" $
         propSizes ws
     it "has a well behaved exclusive lock" $
         lockTest ws

testNS :: NameSpace
testNS = NameSpace "PONIES"

-- Testing locks is hard, you will probably want to prove yours in some other
-- way.
--
-- However, this might catch pathologically bad locks. Increasing the
-- threadDelay for a more latent store might help you find bugs.
lockTest :: Store s => With s -> Expectation
lockTest ws = ws $ \s -> do
    writeCtr s 0

    as <- replicateM 100 . async . withExclusiveLock s testNS "a_lock" $ do
        n <- readCtr s
        threadDelay 1
        writeCtr s (succ n)

    bs <- replicateM 100 . async . withSharedLock s testNS "a_lock" $ do
        n <- readCtr s
        threadDelay 1
        n' <- readCtr s
        when (n /= n') (error "oh noes, lock didn't lock!")

    mapM_ wait (as ++ bs)

    readCtr s >>= (`shouldBe` 100)
  where
    ctr :: ObjectName
    ctr = "counter"


    writeCtr :: Store s => s -> Int -> IO ()
    writeCtr s n = write s testNS [(ctr, S.pack . show $ n)]

    readCtr :: Store s => s -> IO Int
    readCtr s = do
        [Just n] <- fetchs s testNS [ctr]
        return . read . S.unpack $ n

propSizes :: Store s
          => With s
          -> NameSpace
          -> NonEmptyList (ObjectName, ByteString)
          -> Property
propSizes ws ns (NonEmpty writes) =
    monadicIO $ do
        let unique_writes = nubBy ((==) `on` fst) writes
        xs <- run . ws $ \s -> do
            write s ns unique_writes
            sizes s ns (map fst unique_writes)
        let expected = map (Just . fromIntegral . S.length . snd) unique_writes
        assert (xs == expected)

propAppendConcat :: Store s
                 => With s
                 -> NameSpace
                 -> ObjectName
                 -> NonEmptyList ByteString
                 -> Property
propAppendConcat ws ns obj (NonEmpty bss) =
    monadicIO $ do
        xs <- run . ws $ \s -> do
            forM_ bss $ \bs ->
                append s ns [(obj, bs)]
            fetchs s ns [obj]
        assert (xs == [Just (mconcat bss)])

-- | This will also test the ordering of duplicate writes.
propAppendWrite :: Store s
                => With s
                -> NameSpace
                -> NonEmptyList (ObjectName, ByteString)
                -> Positive Int
                -> Property
propAppendWrite ws ns (NonEmpty writes) (Positive rand) =
    monadicIO $ do
        -- We want only the last writes to each object
        let unique_writes = reverse . nubBy ((==) `on` fst) . reverse $ writes
        xs <- run . ws $ \s -> do
            let appends = take (length writes `mod` rand) unique_writes
            append s ns appends
            write s ns writes
            fetchs s ns (map fst unique_writes)
        assert (xs == map (Just . snd) unique_writes)
