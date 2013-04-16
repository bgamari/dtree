import Data.DIntMap as DIM
import Control.Monad
import Data.Hashable

xs :: [Int]
xs = [0..3000000]

f = hash
main = do
    dmap <- DIM.new "hello" :: IO (DIM.DIntMap String)
    root <- DIM.getRoot dmap
    root' <- foldM (\t i->DIM.insert dmap (f i) (replicate 256 'a') t) root xs
    DIM.putRoot dmap root'
    --putStrLn $ showTree root'
    --mapM_ (\i->DIM.lookup (f i) dmap >>= print) xs
    DIM.lookup (f $ head xs) dmap >>= print
