--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

module Main where
import qualified Data.ByteString.Char8 as S
import Options.Applicative
import TimeStore

data Options
    = Options { _pool :: String
              , _user :: Maybe String
              , _ns   :: NameSpace
              , _cmd  :: Action }

data Action
    = Register { simpleBuckets   :: Bucket
               , extendedBuckets :: Bucket
               }

-- | Command line option parsing
helpfulParser :: ParserInfo Options
helpfulParser = info (helper <*> optionsParser) fullDesc

optionsParser :: Parser Options
optionsParser = Options <$> parsePool
                        <*> parseUser
                        <*> parseNS
                        <*> parseAction
  where
    parsePool = strOption $
           long "pool"
        <> short 'p'
        <> metavar "POOL"
        <> help "Ceph pool name for storage"

    parseUser = optional . strOption $
           long "user"
        <> short 'u'
        <> metavar "USER"
        <> help "Ceph user for access to storage"

    parseNS = option $
        long "origin"
        <> short 'o'
        <> help "Origin (namespace)"

    parseAction = subparser parseRegisterAction

    parseRegisterAction =
        componentHelper "register" registerActionParser "Register an "

    componentHelper cmd_name parser desc =
        command cmd_name (info (helper <*> parser) (progDesc desc))


registerActionParser :: Parser Action
registerActionParser = Register <$> parseSimpleBuckets
                                <*> parseExtendedBuckets
  where
    parseSimpleBuckets = option $
        long "simple_buckets"
        <> short 's'
        <> value 0
        <> showDefault
        <> help "Number of simple buckets"

    parseExtendedBuckets = option $
        long "extendend_buckets"
        <> short 'e'
        <> value 0
        <> showDefault
        <> help "Number of extended buckets"

main :: IO ()
main = do
    Options pool user ns cmd <- execParser helpfulParser

    case cmd of
        Register s_buckets e_buckets -> do
            s <- radosStore (fmap S.pack user)
                            "/etc/ceph/ceph.conf"
                            (S.pack pool)
                            0
            registered <- isRegistered s ns
            if registered
                then putStrLn "Origin already registered"
                else registerNamespace s ns s_buckets e_buckets
