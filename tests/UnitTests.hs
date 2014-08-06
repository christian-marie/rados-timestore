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
import Control.Lens hiding (Simple, Index)
import Data.ByteString.Builder

main :: IO ()
main =
    hspec . parallel $ do
        describe "algorithms" $ do
            it "groups simple points" groupSimple
            it "groups extended points" groupExtended


groupSimple :: Expectation
groupSimple =
    groupMixed simpleIndex extendedIndex simplePoints `shouldBe` expected
  where
    expected :: ( Map (Epoch, Bucket) SimpleWrite
                , Map (Epoch, Bucket) ExtendedWrite
                , Map (Epoch, Bucket) PointerWrite
                , Tagged Simple Time                   -- Latest simple write
                , Tagged Extended Time)                -- Latest extended write
    expected = ( Map.fromList [ ((0,0), bucket0_0) :: ((Epoch, Bucket), SimpleWrite)
                              , ((0,2), bucket0_2)
                              , ((6,8), bucket6_8)
                              ]
               , mempty
               , mempty
               , 8
               , 0
               )

    bucket0_0 = fromString . concat $
        [ "\x00\x00\x00\x00\x00\x00\x00\x00" -- Point 0 0 0
        , "\x00\x00\x00\x00\x00\x00\x00\x00"
        , "\x00\x00\x00\x00\x00\x00\x00\x00"
        , "\x04\x00\x00\x00\x00\x00\x00\x00" -- Point 4 4 0 (4 % 4 == 0)
        , "\x04\x00\x00\x00\x00\x00\x00\x00"
        , "\x00\x00\x00\x00\x00\x00\x00\x00"
        ]

    bucket0_2 = fromString . concat $
        [ "\x02\x00\x00\x00\x00\x00\x00\x00" -- Point 2 2 0
        , "\x02\x00\x00\x00\x00\x00\x00\x00"
        , "\x00\x00\x00\x00\x00\x00\x00\x00"
        ]

    bucket6_8 = fromString . concat $
        [ "\x08\x00\x00\x00\x00\x00\x00\x00" -- Point 8 8 0
        , "\x08\x00\x00\x00\x00\x00\x00\x00"
        , "\x00\x00\x00\x00\x00\x00\x00\x00"
        ]

groupExtended :: Expectation
groupExtended = do
    let x@(_, _, ptrs, _, _)  = groupMixed simpleIndex extendedIndex extendedPoints
    x `shouldBe` expected
    print $ ptrs & traverse %~ (\(PointerWrite _ f) -> toLazyByteString $ f 0 Map.empty)
  where
    expected = ( mempty
               , Map.fromList [ ((0, 0), bucket0_0)
                              , ((0, 2), bucket0_2)
                              ]
               , Map.fromList [ ((0, 0), PointerWrite 24 undefined)
                              , ((0, 2), PointerWrite 12 undefined)
                              ]
               , 2
               , 2
               )
    bucket0_0 = fromString . concat $
        [ "\x03\x00\x00\x00\x00\x00\x00\x00" -- Length
        , "hai"                              -- Payload
        , "\x05\x00\x00\x00\x00\x00\x00\x00" -- Length
        , "there"                            -- Payload
        ]

    bucket0_2 = fromString . concat $
        [ "\x04\x00\x00\x00\x00\x00\x00\x00" -- Length
        , "pony"                             -- Payload
        ]

simplePoints :: ByteString
simplePoints = vectorToByteString [Point 0 0 0, Point 2 2 0, Point 4 4 0, Point 8 8 0]

extendedPoints :: ByteString
extendedPoints =  vectorToByteString [Point 1 1 3] <> "hai"
               <> vectorToByteString [Point 1 2 5] <> "there"
               <> vectorToByteString [Point 3 1 4] <> "pony"

simpleIndex :: Tagged Simple Index
simpleIndex = Tagged [ (0, 4) :: (Epoch, Bucket)
                     , (6, 10)
                     ]

extendedIndex :: Tagged Extended Index
extendedIndex = Tagged [(0 :: Epoch, 3 :: Bucket)]