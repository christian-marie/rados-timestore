--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module TimeStore.Stores.Memory
(
    MemoryStore,
    memoryStore,
) where

import TimeStore.Core
import Control.Applicative
import Data.Monoid
import Control.Concurrent hiding (yield)
import Data.ByteString(ByteString)
import Control.Monad
import Data.Map.Strict(Map)
import Pipes
import qualified Data.Map.Strict as Map
import qualified Data.ByteString as S

newtype Key = Key ByteString
  deriving (Eq, Ord)

key :: NameSpace -> ObjectName -> Key
key (NameSpace ns) (ObjectName on) = Key (S.append ns on)

data MemoryStore
    = MemoryStore (MVar (Map Key ByteString)) (MVar [LockName])

memoryStore :: IO MemoryStore
memoryStore = MemoryStore <$> newMVar mempty <*> newMVar mempty

instance Store MemoryStore where
    append = mergeWith (flip S.append)
    write = mergeWith const
    fetch (MemoryStore objects _) ns obj_names =
        forM_ obj_names $ \obj_name ->
            lift (withMVar objects (return . Map.lookup (key ns obj_name)))
            >>= yield
    sizes (MemoryStore objects _) ns obj_names =
        forM obj_names $ \obj_name ->
            withMVar objects $ \obj_map ->
                return $ fromIntegral . S.length
                         <$> Map.lookup (key ns obj_name) obj_map
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
