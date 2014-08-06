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

import Control.Lens hiding (Index, Simple)
import Data.ByteString (ByteString)
import Data.ByteString.Builder
import qualified Data.ByteString.Lazy as L
import qualified Data.Map as Map
import Data.Map.Strict (Map)
import Data.Monoid
import Data.String
import Data.Tagged
import Data.Vector.Storable.ByteString (vectorToByteString)
import Data.Word (Word64)
import Test.Hspec
import TimeStore.Algorithms
import TimeStore.Core
import TimeStore.Index

main :: IO ()
main =
    hspec . parallel $ do
        describe "algorithms" $ do
            it "groups simple points" groupSimple
            it "groups extended points" groupExtended


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
        ( Map.fromList [ ((0,0), bucket0_0) :: ((Epoch, Bucket), SimpleWrite)
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
    x `shouldBe` grouped
    let s_wr = ptrs & traverse %~ (\(PointerWrite _ f) -> toLazyByteString $ f 0 oss)
    s_wr `shouldBe` simpleWrites
  where
    -- | Our extended offsets
    oss :: Map (Epoch, Bucket) Word64
    oss = Map.fromList [ ((0,0), 10), ((0,2), 10) ]

    simpleWrites :: Map (Epoch, Bucket) L.ByteString
    simpleWrites =
        Map.fromList [ ((0,0), ptrs_0_0 )
                     , ((0,2), ptrs_0_2 )
                     ]
      where
        ptrs_0_0 = fromString . concat $
            [ "\x01\x00\x00\x00\x00\x00\x00\x00" -- Address
            , "\x01\x00\x00\x00\x00\x00\x00\x00" -- Time
            , "\x0a\x00\x00\x00\x00\x00\x00\x00" -- Offset (base os of 10 + 0)
            , "\x01\x00\x00\x00\x00\x00\x00\x00" -- Address
            , "\x02\x00\x00\x00\x00\x00\x00\x00" -- Time
            , "\x15\x00\x00\x00\x00\x00\x00\x00" -- Offset (base os of 10 + 11)
            ]                                    -- where 11 is 8 bytes + "hai"

        ptrs_0_2 = fromString . concat $
            [ "\x03\x00\x00\x00\x00\x00\x00\x00" -- Address
            , "\x01\x00\x00\x00\x00\x00\x00\x00" -- Time
            , "\x0a\x00\x00\x00\x00\x00\x00\x00" -- Offset (10 + 0)
            ]

    grouped =
        ( mempty
        , Map.fromList [ ((0, 0), bucket0_0) :: ((Epoch, Bucket), ExtendedWrite)
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
