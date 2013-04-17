{-# LANGUAGE DeriveGeneric, FlexibleContexts, FlexibleInstances, TypeSynonymInstances #-}

module Data.DIntMap ( DIntMap
                    , MemTree, DiskTree
                    , empty
                    , insert, insertWith
                    , lookup
                    , writeTree
                    , getRoot, putRoot
                    , new
                    , open
                    , showTree
                    ) where

import Prelude hiding (mapM, mapM_, lookup, concatMap, last)
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Foldable
import Data.Traversable
import Data.Monoid
import Control.Applicative hiding (empty)
import Control.Monad (void, replicateM, when)
import qualified Data.IntMap.Strict as IM
import Data.IntMap (Key)

import qualified Data.DiskStore as DS

maxBreadth :: Int
maxBreadth = 1024

data Bin a = Bin !(IM.IntMap a) !a
           deriving (Show)

instance Functor Bin where
    fmap f (Bin rest last) = Bin (fmap f rest) (f last)
instance Foldable Bin where
    foldMap f (Bin rest last) = foldMap f rest `mappend` f last
instance Traversable Bin where
    traverse f (Bin rest last) = Bin <$> traverse f rest <*> f last

lookupBin :: Int -> Bin a -> a
lookupBin k (Bin rest last) =
    case IM.lookupGT k rest of
      Nothing     -> last
      Just (_,b)  -> b

insertBin :: Int -> a -> Bin a -> Bin a
insertBin k v (Bin rest last) = Bin (IM.insert k v rest) last

updateBin :: (a -> a) -> Int -> Bin a -> Bin a
updateBin f k (Bin rest last) =
    case IM.lookupGT k rest of
      Just (k',b) -> Bin (IM.adjust f k' rest) last
      Nothing     -> Bin rest (f last)

binSize :: Bin a -> Int
binSize (Bin rest _) = IM.size rest + 1

-- Children keys are greater than or equal to container's key
data Tree v = Leaf { _tNValues :: !Int
                   , _tValues :: !(IM.IntMap v) }
            | Internal { _tNChildren :: !Int
                       , _tChildren :: !(Bin (ObjUpdate (Tree v))) }
            deriving (Show)

empty :: Tree v
empty = Leaf 0 IM.empty

type DiskTree v = Tree v
type MemTree v = Tree v
data ObjUpdate a = RefObj !(DS.Obj a) !(Maybe a)
                 | NewObj !a
                 deriving (Show)

instance Binary (ObjUpdate a) where
    put (RefObj ref _) = put ref
    put (NewObj _)     = error "DIntMap: Tried to serialize NewObj"
    get = RefObj <$> get <*> pure Nothing

instance (Binary v) => Binary (DiskTree v) where
    put (Leaf n values) = do putWord8 0
                             putWord32le $ fromIntegral n
                             mapM_ put $ IM.toAscList values
    put (Internal n (Bin children last)) = do
        putWord8 1
        putWord32le $ fromIntegral (n-1)
        mapM_ put $ IM.toAscList children
        when (IM.size children /= n-1) $ fail $ "Size incorrect "++show (IM.size children, n)
        put last
    get = do type_ <- getWord8
             case type_ of
                 0 -> do n <- fromIntegral <$> getWord32le
                         Leaf n . IM.fromDistinctAscList <$> replicateM n get
                 1 -> do n <- fromIntegral <$> getWord32le
                         children <- IM.fromDistinctAscList <$> replicateM n get
                         last <- get
                         return $ Internal (n+1) (Bin children last)
                 i -> fail $ "DIntMap: Unknown tree type ("++show i++")"

data DIntMap v = DIntMap { _store :: DS.DiskStore (DiskTree v) }

lookup :: (Binary v) => Key -> DIntMap v -> IO (Maybe v)
lookup k dmap = getRoot dmap >>= lookup' dmap k

lookup' :: (Binary v) => DIntMap v -> Key -> MemTree v -> IO (Maybe v)
lookup' dmap k (Leaf _ values) = return $ IM.lookup k values
lookup' dmap k tree = do
    child <- readChild dmap k tree
    case child of
      Just tree -> lookup' dmap k tree
      Nothing   -> return Nothing

-- | Divide an IntMap into two pieces of roughly equal size
divide :: IM.IntMap a -> (IM.IntMap a, IM.IntMap a)
divide m | IM.null m = (IM.empty, IM.empty)
divide m = let n = IM.size m
               (k,_) = head $ drop (n `div` 2) $ IM.toAscList m
               (a,x,b) = IM.splitLookup k m
           in (maybe a (\v->IM.insert k v a) x, b)

insert :: (Binary v) => DIntMap v -> Key -> v -> MemTree v -> IO (MemTree v)
insert dmap = insertWith dmap (\_ a->a)

data Insert v = Ok !(MemTree v)
              | Split !Key !(MemTree v) !(MemTree v)
              deriving (Show)

insertWith :: (Binary v)
           => DIntMap v -> (v -> v -> v) -> Key -> v -> MemTree v -> IO (MemTree v)
insertWith dmap f k v tree = do
    s <- insertWith' dmap f k v tree
    case s of
      Ok tree'  -> return tree'
      Split kl l r -> return $ Internal 2 $ Bin (IM.singleton kl (NewObj l)) (NewObj r)

insertWith' :: (Binary v)
            => DIntMap v -> (v -> v -> v) -> Key -> v -> MemTree v -> IO (Insert v)
--- We are on a leaf node, insert into it
insertWith' _ f k v (Leaf n values)
    -- We need to split
  | n' > maxBreadth = let (l,r) = divide values'
                          nl = IM.size l
                          nr = n' - nl
                          Just ((kl,_),r') = IM.minViewWithKey r
                      in if nl /= IM.size l || nr /= IM.size r
                           then error ("size mismatch "++show (n,nl,nr,n',IM.size values,IM.size values'))
                           else return $ Split kl (Leaf nl l) (Leaf nr r)
    -- We have enough room
  | otherwise      = return $ Ok $ Leaf n' values'
  where (oldV, values') = IM.insertLookupWithKey (const f) k v values
        n' = maybe (n+1) (const $ n) oldV

-- We are on an internal node
insertWith' dmap f k v (Internal n children) = do
    maybeChild <- readChild dmap k (Internal n children)
    case maybeChild of
      Nothing -> error "DIntMap: Internal node not found"
      Just child -> do
        insert <- insertWith' dmap f k v child
        case insert of
          Ok child'     -> let children' = updateBin (const $ NewObj child') k children
                           in return $ Ok $ Internal n children'
          Split kl l r  -> let kr = k
                               children' = insertBin kl (NewObj l)
                                         $ updateBin (const $ NewObj r) kr
                                         $ children
                               n' = binSize children'
                           in return $ splitInternal $ Internal n' children'

  where splitInternal :: Tree v -> Insert v
        splitInternal (Leaf _ _) = error "DIntMap: splitInternal can't split Leafs"
        splitInternal (Internal n (Bin children c))
          | n > maxBreadth =
            let (l,r) = divide children
                Just ((kr', r'), _) = IM.minViewWithKey r
                nl = IM.size l + 1
                --nr = n - nl -- FIXME
                nr = IM.size r + 1
            in Split kr' (Internal nl (Bin l r')) (Internal nr (Bin r c))
          | otherwise      = Ok $ Internal n (Bin children c)

writeChildren :: (Binary v) => DIntMap v -> MemTree v -> IO (DiskTree v)
writeChildren _                 (Leaf n values)       = return (Leaf n values)
writeChildren dmap@(DIntMap store) (Internal n children) =
    Internal n <$> mapM writeChild children
  where writeChild (NewObj a) = do written <- writeChildren dmap a
                                   a' <- DS.appendObj store written
                                   return $ RefObj a' (Just a)
        writeChild a = return a

writeTree :: (Binary v) => DIntMap v -> MemTree v -> IO (DiskTree v, DS.Obj (DiskTree v))
writeTree dmap@(DIntMap store) tree = do
    tree' <- writeChildren dmap tree
    obj <- DS.appendObj store tree'
    return (tree', obj)

-- | Fetch the child node corresponding to the given key (if such node exists)
readChild :: (Binary v)
          => DIntMap v -> Key -> MemTree v -> IO (Maybe (MemTree v))
readChild (DIntMap store) _ (Leaf _ _)          = return Nothing
readChild (DIntMap store) k (Internal _ children) =
    case lookupBin k children of
        (NewObj obj)          -> return $ Just obj
        (RefObj _ (Just obj)) -> return $ Just obj
        (RefObj ref Nothing)  -> do obj <- DS.getObj store ref
                                    return $ Just obj

putRoot :: (Binary v) => DIntMap v -> MemTree v -> IO (DiskTree v)
putRoot dmap@(DIntMap store) tree = do
    (tree', obj) <- writeTree dmap tree
    void $ DS.setRoot store (Just obj)
    return tree'

getRoot :: (Binary v) => DIntMap v -> IO (MemTree v)
getRoot (DIntMap store) = maybe (error "DIntMap: Empty root") id <$> DS.getRoot store

showTree :: (Show v) => (Tree v) -> String
showTree = unlines . go
  where go (Leaf _ values) =
            ["leaf size="++show (IM.size values)]++values'
          where values' = map ("| "++) $ concatMap showKeyValue $ IM.toAscList values
                showKeyValue (k,v) = ["key="++show k++"  ->  "++show v]
        go (Internal _ (Bin rest last)) =
            ["Internal size="++show (1+IM.size rest)]++children'
          where children' = map ("| "++)
                            $ concatMap showChild
                            $ (map (\(k,v)->(show k, v)) $ IM.toAscList rest)++[("last",last)]

        showChild :: (Show v) => (String, ObjUpdate (Tree v)) -> [String]
        showChild (k, NewObj a)   = ["New child key="++k]++map ("  "++) (go a)
        showChild (k, RefObj a b) = ["Ref child key="++k++" to "++show a]++child
          where child = case b of
                  Nothing -> []
                  Just c  -> map ("| "++) $ go c

new :: Binary v => FilePath -> IO (DIntMap v)
new fname = do
    store <- DS.new fname
    a <- DS.appendObj store empty
    void $ DS.setRoot store (Just a)
    return $ DIntMap store

open :: Binary v => FilePath -> IO (Maybe (DIntMap v))
open fname = fmap DIntMap <$> DS.open fname
