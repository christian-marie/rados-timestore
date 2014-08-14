--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

-- | This module provides an abstraction on top of the "regular" TimeStore API
-- to provide "mutability" by viewing a stored piece of data as a series of
-- updates. There is no time associated with these blobs of data, the time
-- becomes a sequence number behind the scenes.
--
-- This module is intended to be imported qualified.
--
module TimeStore.Mutable
(
    lookup,
    insert,
    insertWith,
) where

import Control.Applicative
import Control.Monad
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Data.Packer
import Data.Tagged
import Data.Word
import Pipes
import qualified Pipes.Prelude as P
import Prelude hiding (lookup, mapM)
import TimeStore
import TimeStore.Algorithms
import TimeStore.Core
import TimeStore.StoreHelpers

lookup :: Store s
       => s
       -> NameSpace
       -> Address
       -> IO (Maybe ByteString)
lookup s ns addr =
    (fmap . fmap) fst (lookup' s ns addr)

lookup' :: Store s
        => s
        -> NameSpace
        -> Address
        -> IO (Maybe (ByteString, Time))
lookup' s ns user_addr = do
    let addr = user_addr `setBit` 0
    let bucket_n = placeBucket mutableBuckets addr
    buckets <- fetchs s ns [ name (SimpleBucketLocation (0, bucket_n))
                           , name (ExtendedBucketLocation (0, bucket_n))]
    case sequence buckets of
        Just [s_bs,e_bs] ->
            let processed = processExtended 0 maxBound [addr] s_bs e_bs in
            if S.null processed
                then return Nothing
                else return . Just  $ findLast processed
        _ -> return Nothing

insert :: Store s
       => s
       -> NameSpace
       -> Address
       -> ByteString
       -> IO ()
insert s ns obj x = void $ insertWith s ns const obj x

insertWith :: Store s
           => s
           -> NameSpace
           -> (ByteString -> ByteString -> ByteString)
           -> Address
           -> ByteString
           -> IO ByteString
insertWith s ns f user_addr new = do
    let addr = user_addr `setBit` 0 -- Ensure address is extended
    x <- lookup' s ns addr

    let (bs, t) = case x of
                Nothing -> (new, 0)
                Just (existing, t') -> (f new existing, succ t')

    writeMutableBlob s ns addr t bs
    return bs

enumerate :: Store s
          => s
          -> NameSpace
          -> Producer ByteString IO ()
enumerate s ns = do
    xs <- lift $ fetchs s ns [ name (SimpleBucketLocation (0,0))
                             , name (ExtendedBucketLocation (0,0))
                             ]
    undefined

-- | Search through an extended burst, returning the last payload and timestamp
-- seen.
findLast :: ByteString -> (ByteString, Time)
findLast chunk = go 0 (error "findLast: bug: empty payload")
  where
    go os prev
        | os >= S.length chunk = prev
        | otherwise =
            let (len, t, bs) = runUnpacking (unpack os) chunk
            in go (os + len + 24) (bs, t)

    unpack os = do
        unpackSetPosition (os + 8) -- Seek to time, ignore address.
        t <- Time <$> getWord64LE
        len <- fromIntegral <$> getWord64LE
        bs <- if len == 0
                       then return S.empty
                       else getBytes len

        return (len, t, bs)
