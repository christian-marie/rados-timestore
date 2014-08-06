--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE MultiParamTypeClasses #-}

-- | Convenient wrapper for working with nested maps: Map k (Map k' v)
module TimeStore.NestedMap
(
    NestedMap(..),
    lookup,
    unionWith,
    insertWith,
) where

import Prelude hiding (lookup)
import Data.Map.Strict(Map)
import Data.Monoid
import Data.Foldable
import Control.Lens
import qualified Data.Map.Strict as Map

newtype NestedMap k k' v = NestedMap { unNestedMap :: Map k (Map k' v) }
  deriving (Monoid, Functor, Show, Foldable, Traversable)

lookup :: (Ord k, Ord k') => (k, k') -> NestedMap k k' a -> Maybe a
lookup (k,k') (NestedMap m) =
    Map.lookup k m >>= Map.lookup k'

unionWith :: (Ord k', Ord k)
          => (v -> v -> v)
          -> NestedMap k k' v
          -> NestedMap k k' v 
          -> NestedMap k k' v
unionWith f (NestedMap m) (NestedMap m') =
    NestedMap $ Map.unionWith (Map.unionWith f) m m'

insertWith :: (Ord k', Ord k)
           => (v -> v -> v) 
           -> (k, k')
           -> v
           -> NestedMap k k' v
           -> NestedMap k k' v
insertWith f (k,k') v (NestedMap m) =
    NestedMap $ Map.insertWith (\_ m' -> Map.insertWith f k' v m') k (Map.singleton k' v) m

instance FunctorWithIndex (k,k') (NestedMap k k')
instance FoldableWithIndex (k, k') (NestedMap k k')
instance TraversableWithIndex (k,k') (NestedMap k k') where
    itraverse f (NestedMap m) =
        NestedMap `fmap` Map.traverseWithKey (\k m' -> Map.traverseWithKey (\k' a -> f (k,k') a) m') m
