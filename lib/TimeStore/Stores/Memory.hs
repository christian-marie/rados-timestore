--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TypeFamilies               #-}

module TimeStore.Stores.Memory
(
    MemoryStore,
    memoryStore,
    dumpMemoryStore,
) where

import Control.Applicative
import Control.Concurrent hiding (yield)
import Control.Lens hiding (Index, Simple)
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Monoid
import Data.Word (Word64)
import Hexdump
import Prelude
import TimeStore.Core

newtype Key = Key ByteString
  deriving (Eq, Ord, Show)

newtype RefCount = RefCount Int
  deriving (Eq, Ord, Show, Enum, Bounded, Real, Num, Integral)

key :: NameSpace -> ObjectName -> Key
key (NameSpace ns) (ObjectName on) = Key ("02_" <> ns <> "_" <> on)

data MemoryStore
    = MemoryStore Word64 (MVar (Map Key ByteString)) (MVar (Map LockName RefCount))

-- | Create a new in-memory store with the provided minimum size for rollover.
memoryStore :: Word64 -> IO MemoryStore
memoryStore threshold = MemoryStore threshold <$> newMVar mempty
                                              <*> newMVar mempty

dumpMemoryStore :: MemoryStore -> IO String
dumpMemoryStore (MemoryStore _ objects locks) = do
    objs <- ifoldlOf itraversed fmt mempty <$> readMVar objects
    lcks <- show <$> readMVar locks
    return $ "MemoryStore: \n" ++ objs ++ "\nLocks:\n" ++ lcks
  where
    fmt (Key k) acc v =
        acc <> S.unpack k <> ":\n" <> prettyHex v <> "\n"


instance Store MemoryStore where
    type FetchFuture = Maybe ByteString
    type SizeFuture = Maybe Word64

    rolloverThreshold (MemoryStore s _ _ )  _ =  s

    append = mergeWith (flip S.append)

    write = mergeWith const

    fetch (MemoryStore _ objects _) ns obj =
        withMVar objects (return . Map.lookup (key ns obj))

    reifyFetch _ = return

    size (MemoryStore _ objects _) ns obj =
            withMVar objects $ \obj_map ->
                return $ fromIntegral . S.length
                         <$> Map.lookup (key ns obj) obj_map

    reifySize _ = return

    unsafeSharedLock s@(MemoryStore _ _ locks) ns x lock f = do
        got_it <- modifyMVar locks $ \ls -> do
            let prev = Map.findWithDefault 0 lock ls 
            return $
                if prev == -1
                    then (ls, False) -- Exclusively locked
                    else (Map.insert lock (succ prev) ls, True)
        if got_it
            then do
                r <- f
                modifyMVar_ locks (return . Map.update (Just . pred) lock)
                return r
            else unsafeExclusiveLock s ns x lock f -- spin

    unsafeExclusiveLock s@(MemoryStore _ _ locks) ns x lock f = do
        got_it <-  modifyMVar locks $ \ls -> do
            let prev = Map.findWithDefault 0 lock ls 
            return $
                if prev /= 0
                    then (ls, False)
                    else (Map.insert lock (-1) ls, True)
        if got_it
            then do
                r <- f
                modifyMVar_ locks (return . Map.delete lock)
                return r
            else unsafeExclusiveLock s ns x lock f -- spin



mergeWith :: (ByteString -> ByteString -> ByteString)
          -> MemoryStore
          -> NameSpace
          -> [(ObjectName, ByteString)]
          -> IO ()
mergeWith f (MemoryStore _ objects _) ns appends =
        forM_ appends $ \(obj_name, bytes) ->
            modifyMVar_ objects $
                return . Map.insertWith f (key ns obj_name) bytes
