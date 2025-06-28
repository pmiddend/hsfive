{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE TupleSections #-}

module HsFive.Bitshuffle (bshufDecompressLz4, DecompressResult (..)) where

import Data.Binary.Get (getWord32be, getWord64be, runGet)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.ByteString.Unsafe (unsafeUseAsCString)
import Debug.Trace (trace)
import Foreign (allocaBytes, castPtr, plusPtr)
import Foreign.C.Types (CLLong (CLLong), CSize (CSize))
import Foreign.Ptr (Ptr)
import System.IO.Unsafe (unsafePerformIO)

foreign import capi "bitshuffle.h bshuf_decompress_lz4" bshufDecompressLz4Raw :: Ptr () -> Ptr () -> CSize -> CSize -> CSize -> IO CLLong

data DecompressResult
  = DecompressError !Int
  | DecompressSuccess {decompressBytes :: !BS.ByteString, decompressBytesConsumed :: !Int}

bshufDecompressLz4 :: BS.ByteString -> Int -> DecompressResult
bshufDecompressLz4 input elemSize =
  unsafePerformIO $ do
    -- Taken from bshuf_h5filter.c
    let (uncompressedSize, realBlockSizeTimesElSize) = runGet ((,) <$> getWord64be <*> getWord32be) (BSL.fromStrict input)
        realBlockSize = realBlockSizeTimesElSize `div` fromIntegral elemSize
    unsafeUseAsCString input $ \inputPtr -> allocaBytes (fromIntegral uncompressedSize) $ \outputPtr -> do
      numberOfBytes <-
        bshufDecompressLz4Raw
          (castPtr (inputPtr `plusPtr` 12))
          (castPtr outputPtr)
          (fromIntegral (uncompressedSize `div` fromIntegral elemSize))
          (fromIntegral elemSize)
          (fromIntegral realBlockSize)
      if numberOfBytes < 0
        then pure (DecompressError (fromIntegral numberOfBytes))
        else do
          packedResult <- BS.packCStringLen (castPtr outputPtr, fromIntegral uncompressedSize)
          pure (DecompressSuccess packedResult (fromIntegral uncompressedSize))
