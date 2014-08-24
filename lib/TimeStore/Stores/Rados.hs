--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}

-- | A store for Ceph using librados
module TimeStore.Stores.Rados
(
    RadosStore(..),
    radosStore,
    cleanupRadosStore,
    withRadosStore
) where

import Control.Applicative
import Control.Exception
import Control.Monad
import Data.ByteString (ByteString)
import Data.Monoid
import Foreign hiding (void)
import System.Rados.Base
import TimeStore.Core

-- | A RadosStore is a connected 'Connection' to Ceph, an 'IOContext'
-- associated with a pool, and a rollover threshold.
data RadosStore = RadosStore Connection IOContext Word64

-- | Create a new 'RadosStore' with the given user, config file, pool and
-- rollover threshold for all NameSpaces.
radosStore :: Maybe ByteString -> FilePath -> ByteString -> Word64 -> IO RadosStore
radosStore user cfg pool threshold = do
    h <- newConnection user
    m_e <- confReadFile h cfg
    case m_e of
        Just e -> throwIO . StoreFailure $ e
        Nothing -> do
            connect h
            ctx <- newIOContext h pool
            return (RadosStore h ctx threshold)

-- | Cleanup the 'Connection' and 'IOContext' associated with the store.
cleanupRadosStore :: RadosStore -> IO ()
cleanupRadosStore (RadosStore h ctx _) = do
    cleanupIOContext ctx
    cleanupConnection h

withRadosStore :: Maybe ByteString
               -> FilePath
               -> ByteString
               -> Word64
               -> (RadosStore -> IO a)
               -> IO a
withRadosStore user cfg pool threshold =
    bracket (radosStore user cfg pool threshold) cleanupRadosStore

oid :: NameSpace -> ObjectName -> ByteString
oid (NameSpace ns) (ObjectName on) =
    "02_" <> ns <> "_" <> on

-- | Helper to map an async rados action over a list of objects.
mapAsync :: IOContext
         -> NameSpace
         -> [(ObjectName, ByteString)]
         -> (IOContext
          -> Completion
          -> ByteString
          -> ByteString
          -> IO (Either RadosError x))
         -> IO ()
mapAsync ctx ns xs f =  do
    cs <- forM xs $ \(obj, bs) -> do
        c <- newCompletion
        m_err <- f ctx c (oid ns obj) bs
        case m_err of
            Left e -> throwIO . StoreFailure $ e
            Right _ -> return c
    mapM_ waitForSafe cs

instance Store RadosStore where
    type FetchFuture RadosStore = (Completion, ByteString)
    type SizeFuture  RadosStore = (Completion, ForeignPtr Word64)

    rolloverThreshold (RadosStore _ _ t) _ = t

    append (RadosStore _ ctx _) ns appends =
        mapAsync ctx ns appends asyncAppend

    write (RadosStore _ ctx _) ns appends =
        mapAsync ctx ns appends asyncWriteFull

    fetch (RadosStore _ ctx _) ns obj sz = do
        c <- newCompletion
        m_err <- asyncRead ctx c (oid ns obj) sz 0
        case m_err of
            Left e -> throwIO . StoreFailure $ e
            Right bs ->
                return (c, bs)

    reifyFetch _ (c, bs) = do
        waitForComplete c
        e <- getAsyncError c
        either (throwIO . StoreFailure) (return . const bs) e


    size (RadosStore _ ctx _) ns obj = do
        c <- newCompletion
        m_err <- asyncStat ctx c (oid ns obj)
        case m_err of
            Left e -> throwIO . StoreFailure $ e
            Right (fp_sz, _) ->
                return (c, fp_sz)

    reifySize _ (c, fp_sz) = do
        waitForComplete c
        e <- getAsyncError c
        case e of
            Left (NoEntity{}) -> return Nothing
            Left err          -> throwIO . StoreFailure $ err
            Right _           -> Just <$> withForeignPtr fp_sz peek

    unsafeExclusiveLock (RadosStore _ ctx _) ns timeout (LockName ln) f = do
        let lock_oid = oid ns (ObjectName ln)
        cookie <- newCookie
        bracket_ (exclusiveLock ctx
                                lock_oid
                                "exclusive"
                                cookie
                                "rados-timestore exclusive lock"
                                (Just timeout)
                                [])
                 (unlock ctx lock_oid "exclusive" cookie >>= missingOK)
                 f

    unsafeSharedLock (RadosStore _ ctx _) ns timeout (LockName ln) f = do
        let lock_oid = oid ns (ObjectName ln)
        cookie <- newCookie
        bracket_ (sharedLock ctx
                             lock_oid
                             "exclusive"
                             cookie
                             "tag"
                             "rados-timestore exclusive lock"
                             (Just timeout)
                             [])
                 (unlock ctx lock_oid "exclusive" cookie >>= missingOK)
                 f
