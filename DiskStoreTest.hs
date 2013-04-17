import Data.DiskStore as DS

main = do
    ds <- DS.new "hello" :: IO (DiskStore [DS.Obj Int])
    os <- mapM (DS.appendObj ds) [1..1000]
    root <- DS.appendObj ds os
    DS.setRoot ds (Just root)
    DS.close ds

    Just ds <- DS.open "hello" :: IO (Maybe (DiskStore [DS.Obj Int]))
    Just root <- DS.getRoot ds
    mapM (DS.getObj ds) root >>= print
    DS.close ds
