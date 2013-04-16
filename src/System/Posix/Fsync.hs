module System.Posix.Fsync (fsync) where

import Foreign.C.Error (throwErrnoIfMinus1_)
import Foreign.C.Types (CInt(..))
import System.Posix.Types (Fd(..))

foreign import ccall "fsync"
       c_fsync :: CInt -> IO CInt

fsync :: Fd -> IO ()
fsync (Fd fd) =  throwErrnoIfMinus1_ "fsync" $ c_fsync fd
