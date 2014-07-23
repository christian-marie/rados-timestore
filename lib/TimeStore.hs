--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

module TimeStore
(
    writeEncoded,
) where

import Control.Lens hiding (Index, index, Simple)
import Data.ByteString (ByteString)
import Data.ByteString.Builder (toLazyByteString)
import Data.ByteString.Lazy (toStrict)
import Data.Map.Strict (Map, unionWith)
import Data.Maybe
import Data.Monoid
import Data.Word (Word64)
import Data.Packer
import TimeStore.Algorithms
import TimeStore.Core
import TimeStore.Index

type Stream = ()

-- | Write a mixed blob of points to the supplied store and namespace.
writeEncoded:: Store s => s -> NameSpace -> ByteString -> IO ()
writeEncoded s ns encoded =
    withLock s ns "write_lock" $ do
        -- Group our writes using the latest indexes
        (s_idx, e_idx) <- getIndexes s ns
        let (s_writes, e_writes, p_writes,
             s_latest, e_latest) = groupMixed s_idx e_idx encoded

        updateLatest s ns s_latest e_latest

        -- We would like to batch up the request for offsets, so we convert a
        -- traversal over the indexes to a lens.
        --
        -- unsafePartsOf is needed so that we can change the return type, it
        -- will explode only if the length of domain to getOffsets is
        -- different to the length of the codomain.
        offsets <- unsafePartsOf (itraversed . withIndex) getOffsets e_writes

        -- Now adjust the offsets in the simple pointers.
        let p_adjusted = traversed %~ applyOffsets offsets $ p_writes
        let s_union = unionWith mappend s_writes p_adjusted

        -- Convert our ObjectGroups to ObjectNames for unioned simple objects.
        let s_objs = traversed %~ buildSimple $ itoList s_union

        -- Same for extended objects, also build the builders here.
        let e_objs = traversed %~ buildExtended $ itoList e_writes

        -, - Stick all our writes into one big list (types are now unified) and
        -- do them at once.
        append s ns $ (s_objs, e_objs) ^.. both . traversed
  where
    -- | Find the offsets of a batch of ExtendedWrites.
    --
    -- This must maintain the invariant that size of the domain equals the size
    -- of the codomain.
    getOffsets :: [(Object Extended, ExtendedWrite)] -> IO [(Object Extended, Word64)]
    getOffsets xs = do
        let objs = map fst xs
        offsets <- sizes s ns (map name objs)
        return $ zip objs (map (fromMaybe 0) offsets)

    applyOffsets :: Map (Object Extended) Word64 -> PointerWrite -> SimpleWrite
    applyOffsets offsets (PointerWrite _ f) = SimpleWrite (f 0 offsets)

    buildExtended :: (Object Extended, ExtendedWrite) -> (ObjectName, ByteString)
    buildExtended = bimap name
                          (toStrict . toLazyByteString . unExtendedWrite)

    buildSimple :: (Object Simple, SimpleWrite) -> (ObjectName, ByteString)
    buildSimple = bimap name
                        (toStrict . toLazyByteString . unSimpleWrite)

updateLatest :: Store s => s -> NameSpace -> Time -> Time -> IO ()
updateLatest s ns (Time s_time) (Time e_time) = withLock s ns "latest_update" $ do
    latests <- fetchNow s ns [simpleLatest, extendedLatest]
    write s ns $ case latests of
        [Just s_bytes, Just e_bytes] ->
            mkWrite s_bytes s_time simpleLatest
            ++ mkWrite e_bytes e_time extendedLatest
        [Nothing, Nothing] ->
            -- First write
            [(simpleLatest, pack s_time), (extendedLatest, pack e_time)]
        _ -> error "updateLatest: did not get both or no latest"
  where
    mkWrite bytes t obj = if parse bytes < t then [(obj, pack t)] else []
    pack x = runPacking 8 (putWord64LE x)
    parse = either (const 0) id . tryUnpacking getWord64LE
    simpleLatest = "simple_latest"
    extendedLatest = "extended_latest"


getIndexes :: Store s => s -> NameSpace -> IO (Index Simple, Index Extended)
getIndexes s ns = do
    ixs <- fetchNow s ns ["simple_days", "extended_days"]
    case ixs of
        [Just s_idx, Just e_idx] -> return (s_idx ^. index, e_idx ^. index)
        [Nothing, Nothing] -> error "Invalid origin" -- TODO: proper exception
        _ -> error "getIndexes: did not get all indexes or no indexes"

readAddrs :: Store s => s -> NameSpace -> [Address] -> IO Stream
readAddrs = undefined
