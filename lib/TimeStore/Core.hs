--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}

module TimeStore.Core
(
    Bucket(..),
    ObjectName(..),
    Nameable(..),
    LockName(..),
    Store(..),
    Point(..),
    address,
    time,
    payload,
    NameSpace(..),
    Epoch(..),
    Address(..),
    Time(..),
    LatestFile,
    Simple,
    Extended,
    SimpleBucketLocation(..),
    ExtendeBucketLocation(..)
) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Control.Lens (makeLenses)
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.String (IsString)
import Data.Tagged
import Data.Word (Word64)
import Foreign.Ptr
import Foreign.Storable
import qualified Pipes.Prelude as Pipes
import System.Posix.Signals
import Text.Printf

-- | A concrete implementation of a storage backend for a time store.
--
-- In production, this should be Ceph, but in development you may want to use
-- an in memory store or file backed store.
--
-- The backend has the notion of globally exclusive advisory locks, atomic
-- appends and reads over namespaces of buckets.
--
-- Minimum definition is append, write, fetch, sizes, unsafeLock
--
-- This has the bonus of clearly enumerating the back-end interactions.
class Store s where
    type FetchFuture :: *
    type SizeFuture :: *

    -- | Append to a series of buckets, each append is atomic
    append :: s -> NameSpace -> [(ObjectName,ByteString)] -> IO ()

    -- | Overwrite a series of buckets
    write :: s -> NameSpace -> [(ObjectName, ByteString)] -> IO ()

    -- | Begin fetching the contents of a bucket
    fetch :: s -> NameSpace -> ObjectName -> IO FetchFuture

    -- | Retrieve the contents of a FetchFuture, Nothing if the bucket didn't
    -- exit
    reifyFetch :: s -> FetchFuture -> IO (Maybe ByteString)

    -- | Fetch a series of buckets in parallel
    fetchs :: s -> NameSpace -> [ObjectName] -> IO [Maybe ByteString]
    fetchs s ns objs = mapM (fetch s ns) objs >>= mapM (reifyFetch s)

    -- | Begin getting the size of a bucket
    size :: s -> NameSpace -> ObjectName -> IO SizeFuture

    -- | Retrieve the size of a SizeFuture, Nothing if the bucket didn't exist.
    reifySize :: s -> SizeFuture -> IO (Maybe Word64)

    -- | Fetch a series of sizes in parallel
    sizes :: s -> NameSpace -> [ObjectName] -> IO [Maybe Word64]
    sizes s ns objs = mapM (size s ns) objs >>= mapM (reifySize s)

    unsafeLock :: s -> NameSpace -> Int -> LockName -> IO a -> IO a

    -- | Safely acquire a lock, if your action takes longer than a (arbitrary)
    -- timeout, then we will be forced to abort() so that nothing is broken.
    withLock :: s -> NameSpace -> LockName -> IO a -> IO a
    withLock s ns ln f = do
        unsafeLock s ns lockTimeout ln $ do
            r <- race (threadDelay (lockTimeout * 1000000)) f
            case r of
                Left () -> do
                    putStrLn "withLock: Aborting due to lock timeout"
                    raiseSignal sigABRT
                    error "withLock: highly improbable"

                Right v -> return v

-- | In order to recover from a possible deadlock, we request that any locks
-- are broken after this timeout.
--
-- As a result we *must* abort any operation that takes longer than this.
lockTimeout :: Int
lockTimeout = 120 -- seconds

-- | An ObjectName can be used to retrieve an object's data from the backend.
-- It corresponds to a part of an object ID in ceph. The other part being the
-- namespace.
class Nameable o where
    name :: o -> ObjectName

-- Phantom types for extra safety
data Simple
data Extended

-- Uninhabited wrapper for finding the location of a latest file.
data LatestFile a

instance Nameable (LatestFile Simple) where
    name _ = "simple_latest"

instance Nameable (LatestFile Extended) where
    name _ = "extended_latest"

newtype ObjectName = ObjectName { unObjectName :: ByteString }
  deriving (IsString)

newtype SimpleBucketLocation = SimpleBucketLocation (Epoch,Bucket)
newtype ExtendeBucketLocation = ExtendedBucketLocation (Epoch,Bucket)

instance Nameable SimpleBucketLocation where
    name (SimpleBucketLocation (e,b)) = bucketLocation e b "simple"

instance Nameable ExtendeBucketLocation where
    name (ExtendedBucketLocation (e,b)) = bucketLocation e b "extended"

bucketLocation :: Epoch -> Bucket -> String -> ObjectName
bucketLocation (Epoch epoch) (Bucket bucket) kind =
    ObjectName . S.pack $ printf "%020d_%020d_%s"
                                 bucket
                                 epoch
                                 kind

newtype Bucket
    = Bucket { unBucket :: Word64 }
  deriving (Eq, Ord, Num, Show)

newtype LockName
    = LockName { unLockName :: ByteString }
  deriving (Eq, Ord, IsString, Show)

newtype NameSpace
    = NameSpace { unNameSpace :: ByteString }
  deriving (Eq, Ord, Show, IsString)

newtype Epoch = Epoch { unEpoch :: Word64 }
  deriving (Eq, Ord, Num, Show)

newtype Address = Address {
    unAddress :: Word64
}
  deriving (Eq, Num, Bounded, Bits, Show, Storable)

newtype Time = Time {
    unTime :: Word64
}
  deriving (Eq, Num, Bounded, Ord, Show, Storable)

data Point
    = Point { _address :: !Address
            , _time    :: !Time
            , _payload :: !Word64
            } deriving (Show, Eq)
makeLenses ''Point

instance Storable Point where
    sizeOf _ = 24
    alignment _ = 8
    peek ptr =
        Point <$> peek (castPtr ptr)
              <*> peek (ptr `plusPtr` 8)
              <*> peek (ptr `plusPtr` 16)
    poke ptr (Point a t p) =  do
        poke (castPtr ptr) a
        poke (ptr `plusPtr` 8 ) t
        poke (ptr `plusPtr` 16 ) p
