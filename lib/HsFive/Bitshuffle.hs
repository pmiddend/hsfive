{-# LANGUAGE CApiFFI #-}

module HsFive.Bitshuffle (bshufDecompressLz4, DecompressResult (..)) where

import qualified Data.ByteString as BS
import Foreign (allocaBytes, castPtr)
import Foreign.C.Types (CLLong (CLLong), CSize (CSize))
import Foreign.Ptr (Ptr)
import System.IO.Unsafe (unsafePerformIO)

foreign import capi "bitshuffle.h bshuf_decompress_lz4" bshufDecompressLz4Raw :: Ptr () -> Ptr () -> CSize -> CSize -> CSize -> IO CLLong

data DecompressResult
  = DecompressError !Int
  | DecompressSuccess {decompressBytes :: !BS.ByteString, decompressBytesConsumed :: !Int}

bshufDecompressLz4 :: BS.ByteString -> Int -> Int -> Int -> DecompressResult
bshufDecompressLz4 input size elemSize blockSize =
  unsafePerformIO $ BS.useAsCString input $ \inputPtr -> allocaBytes (fromIntegral size * fromIntegral elemSize) $ \outputPtr -> do
    numberOfBytes <- bshufDecompressLz4Raw (castPtr inputPtr) (castPtr outputPtr) (fromIntegral size) (fromIntegral elemSize) (fromIntegral blockSize)
    if numberOfBytes < 0
      then pure (DecompressError (fromIntegral numberOfBytes))
      else do
        packedResult <- BS.packCStringLen (castPtr outputPtr, fromIntegral numberOfBytes)
        pure (DecompressSuccess packedResult (fromIntegral numberOfBytes))
