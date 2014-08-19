--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Lens hiding (Index, Simple, index)
import Control.Monad
import Data.ByteString (ByteString)
import Data.ByteString.Builder
import qualified Data.ByteString.Lazy as L
import qualified Data.Map as Map
import Data.Map.Strict (Map)
import Data.Monoid
import Data.String
import Data.Tagged
import Data.Vector.Storable (Vector)
import Data.Vector.Storable.ByteString
import Data.Word (Word64)
import Pipes (Producer)
import qualified Pipes.Prelude as Pipes
import Test.Hspec
import TimeStore
import TimeStore.Algorithms
import TimeStore.Core
import TimeStore.Index
import qualified TimeStore.Mutable as MutableTS

main :: IO ()
main =
    hspec $ do
        describe "algorithms" $ do
            it "groups simple points" groupSimple
            it "groups extended points" groupExtended

        describe "main api" $ do
            describe "corner cases" $
                it "handles ext write after simple roll-over" extAfterSimple

            describe "registerOrigin" $
                it "writes index files out" registerWritesIndex

            describe "writeEncoded" $
                it "mixed blob writes expected buckets" writeEncodedBlob

            describe "readSmple" $
                it "reads simple points correctly" readSimplePoints

            describe "readExtended" $
                it "reads extended points correctly" readExtendedPoints

        describe "mutable API" $
            describe "insert/lookup" $
                it "overwrites and reads" overwriteThenReadMutable

extAfterSimple :: Expectation
extAfterSimple = do
        s <- memoryStore 0
        registerNamespace s testNS 4 5
        let e_write = vectorToByteString [Point 7 7 5] <> "yayyy"
        -- Rollover at 6 (nanoseconds)
        writeEncoded s testNS (vectorToByteString [Point 6 6 3])
        -- Now, this may fail if there is incorectness in any extended/simple
        -- indexing/rollover logic.
        writeEncoded s testNS e_write
        Pipes.head (readExtended s testNS 0 maxBound [7])
            >>= (`shouldBe` Just e_write)

overwriteThenReadMutable :: Expectation
overwriteThenReadMutable = do
    s <- memoryStore 0
    registerNamespace s testNS 3 2

    MutableTS.lookup s testNS 0 >>=
        (`shouldBe` Nothing)
    MutableTS.insert s testNS 0 "Hi."
    MutableTS.insert s testNS 0 "I'm a duck"
    MutableTS.lookup s testNS 0 >>=
        (`shouldBe` Just "I'm a duck")

    Pipes.toListM (MutableTS.enumerate s testNS) >>=
        (`shouldBe` [vectorToByteString [Point 1 1 10] <> "I'm a duck"])

testNS :: NameSpace
testNS = NameSpace "PONIES"

readSimplePoints :: Expectation
readSimplePoints = do
    s <- memoryStore 0
    registerNamespace s testNS 10 20
    writeEncoded s testNS simplePoints
    writeEncoded s testNS extraSimples

    pipeToVec (readSimple s testNS 0 21 []) >>=
        (`shouldBe` [])

    pipeToVec (readSimple s testNS 0 21 [2]) >>=
        (`shouldBe` [[Point 2 2 0]])

    pipeToVec (readSimple s testNS 0 21 [2]) >>=
        (`shouldBe` [[Point 2 2 0]])

    pipeToVec (readSimple s testNS 0 21 [14, 4, 6, 8]) >>=
        (`shouldBe` [ [Point 4 4 0]
                    , [Point 8 8 0]
                    , [Point 14 18 0, Point 4 20 0] -- mod 10
                    , [Point 6 15 0]
                    ])

readExtendedPoints :: Expectation
readExtendedPoints = do
    s <- memoryStore 0
    registerNamespace s testNS 5 10
    writeEncoded s testNS extendedPoints

    Pipes.toListM (readExtended s testNS 0 21 []) >>=
        (`shouldBe` [])

    Pipes.toListM (readExtended s testNS 0 21 [1]) >>=
        (`shouldBe` [expected1])

    Pipes.toListM (readExtended s testNS 0 21 [1, 3]) >>=
        (`shouldBe` [expected1, expected2])
  where
    expected1 = vectorToByteString [Point 1 1 3] <> "hai"
             <> vectorToByteString [Point 1 2 5] <> "there"

    expected2 = vectorToByteString [Point 3 1 4] <> "pony"


pipeToVec :: Monad m => Producer ByteString m () -> m [Vector Point]
pipeToVec = liftM (map byteStringToVector) . Pipes.toListM

registerWritesIndex :: Expectation
registerWritesIndex = do
    s <- memoryStore 0
    registerNamespace s testNS 10 20
    ixs <- fetchIndexes s testNS
    ixs `shouldBe` Just (Tagged [(0, 10)], Tagged [(0,20)])

writeEncodedBlob :: Expectation
writeEncodedBlob = do
    s <- memoryStore 0
    -- Write the indexes as our previous tests expect to disk.
    write s testNS [ ( name (undefined :: Tagged Simple Index)
                     , untag simpleIndex ^. from index
                     )
                   , ( name (undefined :: Tagged Extended Index)
                     , untag extendedIndex ^. from index
                     )
                   ]
    writeEncoded s testNS (simplePoints <> extendedPoints)
    objects <- fetchs s testNS [ name (SimpleBucketLocation (0,0))
                               , name (SimpleBucketLocation (0,2))
                               , name (SimpleBucketLocation (6,8))
                               , name (ExtendedBucketLocation (0,0))
                               , name (ExtendedBucketLocation (0,2))
                               , name (undefined :: Tagged Simple Index)
                               , name (undefined :: Tagged Extended Index)
                               , name (undefined :: LatestFile Simple)
                               , name (undefined :: LatestFile Extended)
                               ]
    case sequence objects of
        Just [s'00, s'02, s'68, e'00, e'02, s_ix, e_ix, s_last, e_last] -> do
            SimpleWrite (byteString s'00) `shouldBe` (s00 <> p00)
            SimpleWrite (byteString s'02) `shouldBe` (s02 <> p02)
            SimpleWrite (byteString s'68) `shouldBe` s68
            ExtendedWrite (byteString e'00) `shouldBe` e00
            ExtendedWrite (byteString e'02) `shouldBe` e02
            -- No rollover as offsets are checked *before* write
            Tagged (e_ix ^.index) `shouldBe` extendedIndex
            -- But the simple points will roll over and add a new entry at
            -- epoch 8 with 10 buckets.
            s_ix ^. index `shouldBe` [ (0, 4), (6, 10), (8, 10) ]

            s_last `shouldBe` "\x08\x00\00\x00\x00\x00\x00\x00"
            e_last `shouldBe` "\x02\x00\00\x00\x00\x00\x00\x00"
        _ ->
            error "failed to fetch one of the buckets" -- have fun finding it

groupSimple :: Expectation
groupSimple =
    groupMixed simpleIndex extendedIndex simplePoints `shouldBe` grouped
  where
    grouped :: ( Map (Epoch, Bucket) SimpleWrite
                , Map (Epoch, Bucket) ExtendedWrite
                , Map (Epoch, Bucket) PointerWrite
                , Tagged Simple Time                   -- Latest simple write
                , Tagged Extended Time)                -- Latest extended write
    grouped =
        ( Map.fromList [ ((0,0), s00) :: ((Epoch, Bucket), SimpleWrite)
                       , ((0,2), s02)
                       , ((6,8), s68)
                       ]
        , mempty
        , mempty
        , 8
        , 0
        )

-- Expected test buckets
--
-- Simple:
s00, s02, s68 :: SimpleWrite
s00 = fromString . concat $
    [ "\x00\x00\x00\x00\x00\x00\x00\x00" -- Point 0 0 0
    , "\x00\x00\x00\x00\x00\x00\x00\x00"
    , "\x00\x00\x00\x00\x00\x00\x00\x00"
    , "\x04\x00\x00\x00\x00\x00\x00\x00" -- Point 4 4 0 (4 % 4 == 0)
    , "\x04\x00\x00\x00\x00\x00\x00\x00"
    , "\x00\x00\x00\x00\x00\x00\x00\x00"
    ]

s02 = fromString . concat $
    [ "\x02\x00\x00\x00\x00\x00\x00\x00" -- Point 2 2 0
    , "\x02\x00\x00\x00\x00\x00\x00\x00"
    , "\x00\x00\x00\x00\x00\x00\x00\x00"
    ]

s68 = fromString . concat $
    [ "\x08\x00\x00\x00\x00\x00\x00\x00" -- Point 8 8 0
    , "\x08\x00\x00\x00\x00\x00\x00\x00"
    , "\x00\x00\x00\x00\x00\x00\x00\x00"
    ]

-- Pointers (also simple writes)
p00, p02 :: SimpleWrite
p00 = fromString . concat $
    [ "\x01\x00\x00\x00\x00\x00\x00\x00" -- Address
    , "\x01\x00\x00\x00\x00\x00\x00\x00" -- Time
    , "\x00\x00\x00\x00\x00\x00\x00\x00" -- Offset (base os of 0 + 0)
    , "\x01\x00\x00\x00\x00\x00\x00\x00" -- Address
    , "\x02\x00\x00\x00\x00\x00\x00\x00" -- Time
    , "\x0b\x00\x00\x00\x00\x00\x00\x00" -- Offset (base os of 0 + 11)
    ]                                    -- where 11 is 8 bytes + "hai"

p02 = fromString . concat $
    [ "\x03\x00\x00\x00\x00\x00\x00\x00" -- Address
    , "\x01\x00\x00\x00\x00\x00\x00\x00" -- Time
    , "\x00\x00\x00\x00\x00\x00\x00\x00" -- Offset 0
    ]

-- And extended buckets, which are separate.
e00, e02 :: ExtendedWrite
e00 = fromString . concat $
    [ "\x03\x00\x00\x00\x00\x00\x00\x00" -- Length
    , "hai"                              -- Payload
    , "\x05\x00\x00\x00\x00\x00\x00\x00" -- Length
    , "there"                            -- Payload
    ]

e02 = fromString . concat $
    [ "\x04\x00\x00\x00\x00\x00\x00\x00" -- Length
    , "pony"                             -- Payload
    ]

writeLazyIso :: Iso' SimpleWrite L.ByteString
writeLazyIso = iso (toLazyByteString . unSimpleWrite)
                   (SimpleWrite . lazyByteString)

groupExtended :: Expectation
groupExtended = do
    let x@(_, _, ptrs, _, _)  = groupMixed simpleIndex extendedIndex extendedPoints
    x `shouldBe` grouped
    let s_wr = ptrs & traverse %~ (\(PointerWrite _ f) -> toLazyByteString $ f 0 oss)
    s_wr `shouldBe` simpleWrites
  where
    -- | Our extended offsets
    oss :: Map (Epoch, Bucket) Word64
    oss = Map.fromList [ ((0,0), 0), ((0,2), 0) ]

    simpleWrites :: Map (Epoch, Bucket) L.ByteString
    simpleWrites =
        Map.fromList [ ((0,0), p00 ^. writeLazyIso)
                     , ((0,2), p02 ^. writeLazyIso)
                     ]
    grouped =
        ( mempty
        , Map.fromList [ ((0, 0), e00) :: ((Epoch, Bucket), ExtendedWrite)
                       , ((0, 2), e02)
                       ]
        , Map.fromList [ ((0, 0), PointerWrite 24 undefined)
                       , ((0, 2), PointerWrite 12 undefined)
                       ]
        , 2
        , 2
        )

simplePoints :: ByteString
simplePoints =
    vectorToByteString [ Point 0 0 0
                       , Point 2 2 0
                       , Point 4 4 0
                       , Point 8 8 0
                       ]

-- Used for testing rollovers, sent in a different packet to simplePoints.
extraSimples :: ByteString
extraSimples =
    vectorToByteString [ Point 0 10 0,
                         Point 4 20 0,
                         Point 14 18 0,
                         Point 6 15 0
                       ]


extendedPoints :: ByteString
extendedPoints = vectorToByteString [Point 1 1 3] <> "hai"
              <> vectorToByteString [Point 1 2 5] <> "there"
              <> vectorToByteString [Point 3 1 4] <> "pony"


extraExtendeds :: ByteString
extraExtendeds = vectorToByteString [Point 1 8 3] <> "wat"


simpleIndex :: Tagged Simple Index
simpleIndex = Tagged [ (0, 4) :: (Epoch, Bucket)
                     , (6, 10)
                     ]

extendedIndex :: Tagged Extended Index
extendedIndex = Tagged [(0 :: Epoch, 3 :: Bucket)]
