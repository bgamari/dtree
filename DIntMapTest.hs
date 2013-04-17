import Data.DIntMap as DIM
import Control.Monad
import Data.Hashable

xs :: [Int]
xs = [0..1000000]

f = hash
main = do
    dmap <- DIM.new "hello" :: IO (DIM.DIntMap Int)
    root <- DIM.getRoot dmap
    root' <- foldM (\t i->DIM.insert dmap (f i) 1 t) root xs
    DIM.putRoot dmap root'
    --putStrLn $ showTree root'
    --mapM_ (\i->DIM.lookup (f i) dmap >>= print) xs
    DIM.lookup (f $ head xs) dmap >>= print
