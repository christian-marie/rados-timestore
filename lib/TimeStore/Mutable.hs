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

import Prelude hiding (lookup)
import TimeStore.Core
import Data.ByteString(ByteString)
import Control.Monad

lookup :: Store s
    => s
    -> NameSpace 
    -> Address
    -> IO (Maybe ByteString)
lookup = undefined

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
insertWith = undefined
