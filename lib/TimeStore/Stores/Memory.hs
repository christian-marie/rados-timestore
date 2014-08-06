--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies               #-}

module TimeStore.Stores.Memory
(
    MemoryStore,
    memoryStore,
) where

import Control.Applicative
import Control.Concurrent hiding (yield)
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Monoid
import Data.Word (Word64)
import TimeStore.Core

newtype Key = Key ByteString
  deriving (Eq, Ord)

key :: NameSpace -> ObjectName -> Key
key (NameSpace ns) (ObjectName on) = Key (S.append ns on)

data MemoryStore
    = MemoryStore (MVar (Map Key ByteString)) (MVar [LockName])

memoryStore :: IO MemoryStore
memoryStore = MemoryStore <$> newMVar mempty <*> newMVar mempty

instance Store MemoryStore where
    type FetchFuture = Maybe ByteString
    type SizeFuture = Maybe Word64

    append = mergeWith (flip S.append)

    write = mergeWith const

    fetch (MemoryStore objects _) ns obj =
        withMVar objects (return . Map.lookup (key ns obj))

    reifyFetch _ = return

    size (MemoryStore objects _) ns obj =
            withMVar objects $ \obj_map ->
                return $ fromIntegral . S.length
                         <$> Map.lookup (key ns obj) obj_map

    reifySize _ = return

    unsafeLock s@(MemoryStore _ locks) ns x lock f = do
        got_it <- modifyMVar locks $ \ls ->
            if lock `elem` ls
                then return (ls, False)
                else return (lock:ls, True)
        if got_it
            then do
                r <- f
                modifyMVar_ locks (return . filter (/= lock))
                return r
            else unsafeLock s ns x lock f


mergeWith :: (ByteString -> ByteString -> ByteString)
          -> MemoryStore
          -> NameSpace
          -> [(ObjectName, ByteString)]
          -> IO ()
mergeWith f (MemoryStore objects _) ns appends =
        forM_ appends $ \(obj_name, bytes) ->
            modifyMVar_ objects $
                return . Map.insertWith f (key ns obj_name) bytes
