{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Data.DHashMap ( -- * On-disk hash maps
                       DHashMap
                     , new
                     , open
                       -- * Manipulation
                     , DHashMapT
                     , runDHashMapT
                     , flushCache
                     , commit
                     , insert
                     ) where

import Control.Monad.State
import Control.Monad.Reader
import Control.Applicative

import Data.Hashable (Hashable, hashWithSalt)
import qualified Data.Binary as B
import Data.Binary (Binary)

import qualified Data.DIntMap as DIM

data DHashMap k v = DHashMap (DIM.DIntMap v)

new :: Binary v => FilePath -> IO (DHashMap k v)
new fname = DHashMap <$> DIM.new fname

open :: Binary v => FilePath -> IO (Maybe (DHashMap k v))
open fname = fmap DHashMap <$> DIM.open fname


newtype DHashMapT k v m a = DHashMapT (StateT (DIM.MemTree v) (ReaderT (DIM.DIntMap v) m) a)
                       deriving (Monad)

runDHashMapT :: (Binary k, Binary v, MonadIO m)
            => DHashMap k v -> DHashMapT k v m a -> m a
runDHashMapT (DHashMap imap) (DHashMapT a) = do
    root <- liftIO $ DIM.getRoot imap
    runReaderT (evalStateT a root) imap

flushCache :: (Binary k, Binary v, MonadIO m) => DHashMapT k v m ()
flushCache = DHashMapT $ lift ask >>= liftIO . DIM.getRoot >>= put

commit :: (Binary k, Binary v, MonadIO m) => DHashMapT k v m ()
commit = DHashMapT $ do dmap <- lift ask
                        s <- get
                        liftIO $ DIM.putRoot dmap s
                        return ()

insert :: (Hashable k, Binary k, Binary v, MonadIO m)
       => k -> v -> DHashMapT k v m ()
insert = insertWith (const id)

insertWith :: (Hashable k, Binary k, Binary v, MonadIO m)
           => (v -> v -> v) -> k -> v -> DHashMapT k v m ()
insertWith f k v = DHashMapT $ do
    dmap <- lift ask
    s <- get
    liftIO (DIM.insertWith dmap f (hashKey k) v s) >>= put

salt = 0xdeadbeef

hashKey :: Hashable k => k -> Int
hashKey = hashWithSalt salt
