--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}

import Test.Hspec
import Control.Monad
import Data.Tagged
import TimeStore.Core
import TimeStore.Index
import TimeStore.Algorithms
import Data.Map.Strict(Map)
import Data.ByteString(ByteString)
import qualified Data.Vector.Storable as V
import Data.Vector.Storable.ByteString(vectorToByteString)
import qualified Data.Map as Map
import Data.Monoid

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
                , Tagged Simple Time
                , Tagged Extended Time)
    expected = ( Map.fromList [((1 :: Epoch,2 :: Bucket), "wat" :: SimpleWrite)]
               , mempty
               , mempty
               , 0
               , 0
               )


simplePoints :: ByteString
simplePoints = vectorToByteString [Point 0 0 0]

simpleIndex :: Tagged Simple Index
simpleIndex = Tagged [(8 :: Epoch, 0 :: Bucket)] 

extendedIndex :: Tagged Extended Index
extendedIndex = Tagged [(4 :: Epoch, 0 :: Bucket)] 
