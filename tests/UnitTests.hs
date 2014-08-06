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

import Data.ByteString (ByteString)
import qualified Data.Map as Map
import Data.Map.Strict (Map)
import Data.Monoid
import Data.String
import Data.Tagged
import Data.Vector.Storable.ByteString (vectorToByteString)
import Test.Hspec
import TimeStore.Algorithms
import TimeStore.Core
import TimeStore.Index

main :: IO ()
main =
    hspec . parallel $ do
        describe "algorithms" $ do
            it "groups simple points only" groupSimpleOnly


groupSimpleOnly :: IO ()
groupSimpleOnly =
    groupMixed simpleIndex extendedIndex simplePoints `shouldBe` expected
  where
    expected :: ( Map (Epoch, Bucket) SimpleWrite
                , Map (Epoch, Bucket) ExtendedWrite
                , Map (Epoch, Bucket) PointerWrite
                , Tagged Simple Time                   -- Latest simple write
                , Tagged Extended Time)                -- Latest extended write
    expected = ( Map.fromList [((0,0) :: (Epoch, Bucket), bucket0)
                              ,((0,2)                   , bucket2)
                              ]
               , mempty
               , mempty
               , 4
               , 0
               )

    bucket0 :: SimpleWrite
    bucket0 = fromString . concat $
        [ "\x00\x00\x00\x00\x00\x00\x00\x00" -- Point 0 0 0
        , "\x00\x00\x00\x00\x00\x00\x00\x00"
        , "\x00\x00\x00\x00\x00\x00\x00\x00"
        , "\x04\x00\x00\x00\x00\x00\x00\x00" -- Point 4 4 4 (4 % 4 == 0)
        , "\x04\x00\x00\x00\x00\x00\x00\x00"
        , "\x04\x00\x00\x00\x00\x00\x00\x00"
        ]

    bucket2 :: SimpleWrite
    bucket2 = fromString . concat $
        [ "\x02\x00\x00\x00\x00\x00\x00\x00" -- Point 4 4 4
        , "\x02\x00\x00\x00\x00\x00\x00\x00"
        , "\x02\x00\x00\x00\x00\x00\x00\x00"
        ]

simplePoints :: ByteString
simplePoints = vectorToByteString [Point 0 0 0, Point 2 2 2, Point 4 4 4]

simpleIndex :: Tagged Simple Index
simpleIndex = Tagged [(0 :: Epoch, 4 :: Bucket)]

extendedIndex :: Tagged Extended Index
extendedIndex = Tagged [(0 :: Epoch, 3 :: Bucket)]
