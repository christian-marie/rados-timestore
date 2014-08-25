--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeFamilies               #-}

module TimeStore.Core
(
    -- * User facing types
    Store(..),
    NameSpace(..),
    nameSpace,
    Address(..),
    address,
    time,
    payload,
    Time(..),

    -- * Internal types
    Bucket(..),
    ObjectName(..),
    Nameable(..),
    LockName(..),
    Point(..),
    Epoch(..),
    LatestFile(..),
    Simple,
    Extended,
    SimpleBucketLocation(..),
    ExtendeBucketLocation(..),

    -- * Utility
    placeBucket,

    -- * Exceptions
    InvalidPayload(..),
    StoreFailure(..),
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
import Data.Traversable (for, traverse)
import Data.Typeable
import Data.Word (Word64)
import Foreign.Ptr
import Foreign.Storable
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
    type FetchFuture s
    type SizeFuture s

    -- | Get the rollover threshould for a given store and namespace. This is
    -- the minumum size that a bucket should be in order for us to select a new
    -- epoch. Default is 4MB.
    rolloverThreshold :: s -> NameSpace -> Word64
    rolloverThreshold _ _ = 2 ^ (20 :: Word64) * 4

    -- | Append to a series of buckets, each append is atomic
    append :: s -> NameSpace -> [(ObjectName,ByteString)] -> IO ()

    -- | Overwrite a series of buckets
    write :: s -> NameSpace -> [(ObjectName, ByteString)] -> IO ()

    -- | Begin fetching the contents of a bucket with the provided size
    fetch :: s -> NameSpace -> ObjectName -> Word64 -> IO (FetchFuture s)

    -- | Retrieve the contents of a FetchFuture
    reifyFetch :: s -> FetchFuture s -> IO ByteString

    -- | Fetch a series of buckets in parallel
    fetchs :: s -> NameSpace -> [ObjectName] -> IO [Maybe ByteString]
    fetchs s ns objs = do
        -- Maybe fetch the sizes for our objects
        m_szs <- sizes s ns objs
        -- For those that worked, begin fetching the contents.
        m_fetches <- for (zip objs m_szs) $ \(obj, m_sz) ->
            traverse (fetch s ns obj) m_sz
        -- Now try to reify the contents for those that have any
        (traverse . traverse) (reifyFetch s) m_fetches

    -- | Begin getting the size of a bucket
    size :: s -> NameSpace -> ObjectName -> IO (SizeFuture s)

    -- | Retrieve the size of a SizeFuture, Nothing if the bucket didn't exist.
    reifySize :: s -> SizeFuture s -> IO (Maybe Word64)

    -- | Fetch a series of sizes in parallel
    sizes :: s -> NameSpace -> [ObjectName] -> IO [Maybe Word64]
    sizes s ns objs = traverse (size s ns) objs >>= traverse (reifySize s)

    -- | Take an exclusive lock with the given expiry. If a lock remains after
    -- this expiry then there are no guarantees that we can recover from a
    -- deadlock.
    unsafeExclusiveLock :: s -> NameSpace -> Double -> LockName -> IO a -> IO a

    -- | A shared lock. Many may hold this lock until an exclusive lock is
    -- taken.
    unsafeSharedLock :: s -> NameSpace -> Double -> LockName -> IO a -> IO a

    -- | Safely acquire a lock, if your action takes longer than a (arbitrary)
    -- timeout, then we will be forced to abort() so that nothing is broken.
    withExclusiveLock :: s -> NameSpace -> LockName -> IO a -> IO a
    withExclusiveLock s ns ln f =
        unsafeExclusiveLock s ns (succ lockTimeout) ln (watchDog "withExclusiveLock" f)

    -- | Safely acquire shared lock.
    withSharedLock :: s -> NameSpace -> LockName -> IO a -> IO a
    withSharedLock s ns ln f =
        unsafeSharedLock s ns (succ lockTimeout) ln (watchDog "withExclusiveLock" f)



-- | Run the action for at most lock timeout time, then
watchDog :: String -> IO a -> IO a
watchDog msg f = msg `seq` do
    r <- race (threadDelay (ceiling lockTimeout * 1000000)) f
    case r of
        Left () -> do
            putStrLn $ msg ++ ": Aborting due to lock timeout"
            raiseSignal sigABRT
            error $ msg ++ ": highly improbable"

        Right v -> return v

-- | In order to recover from a possible deadlock, we request that any locks
-- are broken after this timeout.
--
-- As a result we *must* abort any operation that takes longer than this.
lockTimeout :: Double
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
data LatestFile a = LatestFile

-- This is where we store an incrementing "pointer" to the latest
-- (chronologically) point we have ever seen.
instance Nameable (LatestFile Simple) where
    name _ = "simple_latest"

instance Nameable (LatestFile Extended) where
    name _ = "extended_latest"

-- | An ObjectName represents a key for a store that can to be associated with
-- data. They are unique within a NameSpace.
newtype ObjectName = ObjectName { unObjectName :: ByteString }
  deriving (IsString, Show, Eq)

-- | The location of a bucket is calculated using the Epoch and Bucket. The
-- bucket is calculated by (address mod max_buckets)
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
  deriving (Eq, Ord, Num, Show, Enum, Real, Integral, Read)

newtype LockName
    = LockName { unLockName :: ByteString }
  deriving (Eq, Ord, IsString, Show)

newtype NameSpace
    = NameSpace { unNameSpace :: ByteString }
  deriving (Eq, Ord, Show)

instance Read NameSpace where
    readsPrec _ =
        pure . (,"") . either error id . nameSpace . S.pack

nameSpace :: ByteString -> Either String NameSpace
nameSpace bs
    | S.null bs     = Left "NameSpace may not be empty"
    | S.elem '_' bs = Left "NameSpace may not include _"
    | otherwise     = Right . NameSpace $ bs

newtype Epoch = Epoch { unEpoch :: Word64 }
  deriving (Eq, Ord, Num, Show)

newtype Address = Address {
    unAddress :: Word64
}
  deriving (Eq, Ord, Num, Bounded, Bits, Show, Storable, Enum, Real, Integral)

newtype Time = Time {
    unTime :: Word64
}
  deriving (Eq, Num, Bounded, Ord, Show, Storable, Enum)

data Point
    = Point { _address :: !Address
            , _time    :: !Time
            , _payload :: !Word64
            } deriving (Show, Eq)
makeLenses ''Point

instance Ord Point where
    -- Compare time first, then address. This way we can de-deplicate by
    -- comparing adjacent values.
    compare (Point a t _) (Point a' t' _) =
        case compare t t' of
            EQ -> compare a a'
            c  -> c

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

-- | Given a maximum number of buckets to hash over, map an address to the
-- corresponding bucket within the epoch.
placeBucket :: Bucket -> Address -> Bucket
placeBucket (Bucket max_buckets) (Address addr) =
    Bucket $ (addr `clearBit` 0) `mod` max_buckets

data InvalidPayload
    = forall e. Exception e => InvalidPayload e
  deriving (Typeable)

data StoreFailure
    = forall e. Exception e => StoreFailure e
  deriving (Typeable)

instance Exception InvalidPayload
instance Exception StoreFailure
deriving instance Show InvalidPayload
deriving instance Show StoreFailure
