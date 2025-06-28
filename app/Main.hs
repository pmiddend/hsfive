{-# LANGUAGE BinaryLiterals #-}
{-# HLINT ignore "Use newtype instead of data" #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

module Main where

-- import Debug.Trace (trace, traceShowId, traceWith)

import Control.Monad (join, replicateM)
import Control.Monad.State (State, evalState, get, put)
import Data.Binary (getWord8)
import Data.Binary.Get (bytesRead, getDoublele, getInt64le, getWord16le, getWord64le, isEmpty, isolate, runGet, runGetOrFail, skip)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSL8
import Data.Int (Int64)
import Data.List (intercalate, sortOn)
import Data.Maybe (mapMaybe)
import Data.Text (unpack)
import Data.Traversable (forM)
import Data.Word (Word32)
import HsFive.Bitshuffle (DecompressResult (DecompressError, DecompressSuccess), bshufDecompressLz4)
import HsFive.CoreTypes
import HsFive.H5Dump (h5dump)
import HsFive.Types (appendPath, goToNode, readH5, readNode, singletonPath)
import Safe (headMay)
import System.File.OsPath (withBinaryFile, withFile)
import System.IO (Handle, IOMode (ReadMode, WriteMode), SeekMode (AbsoluteSeek, SeekFromEnd), hPutStrLn, hSeek, hTell)
import System.OsPath (OsPath, encodeUtf)

trace :: String -> a -> a
trace x f = f

traceWith :: (a -> String) -> a -> a
traceWith x f = f

readSuperblock' :: Handle -> Integer -> Integer -> IO (Maybe Superblock)
readSuperblock' handle fileSize start = do
  putStrLn $ "trying at " <> show start
  hSeek handle AbsoluteSeek start
  superblockCandidate <- BSL.hGet handle 2048
  case runGetOrFail getSuperblock superblockCandidate of
    Left _ ->
      let newStart = if start == 0 then 512 else start * 2
       in if newStart + 8 >= fileSize
            then pure Nothing
            else readSuperblock' handle fileSize newStart
    Right (_, _, superblock) -> pure (Just superblock)

readSuperblock :: Handle -> IO (Maybe Superblock)
readSuperblock handle = do
  hSeek handle SeekFromEnd 0
  fileSize <- hTell handle
  readSuperblock' handle fileSize 0

main :: IO ()
main = do
  let inputBytes = BS.pack [0, 0, 0, 9, 128, 210, 208, 222, 157, 64, 255, 223, 0, 114, 108, 100, 0]
      decompressed = bshufDecompressLz4 inputBytes 12 1 0

  case decompress of
    DecompressError _ -> putStrLn "error decompressing"
    DecompressSuccess bytes numberBytes ->
      putStrLn ("unpacked " <> show numberBytes <> " bytes: " <> show (BS.unpack bytes))

  let inputFile :: String
      inputFile = "/home/pmidden/Downloads/water_224.h5"

  fileNameEncoded <- encodeUtf inputFile
  rootGroup <- readH5 fileNameEncoded
  putStrLn $ "node: " <> show rootGroup
  putStrLn $ "node: " <> show (goToNode rootGroup (appendPath (singletonPath "/") "entry_0000"))

  putStrLn $ unpack $ h5dump rootGroup

-- fileNameEncoded <- encodeUtf inputFile
-- graph <- readH5ToGraph fileNameEncoded
-- withBinaryFile fileNameEncoded ReadMode $ \handle -> do
--   readChunkedLayouts handle graph

-- outputFile <- encodeUtf "samples/graph_water_224.dot"
-- withFile outputFile WriteMode $ \handle -> do
--   hPutStrLn handle (graphToDot graph)

-- outputFile <- encodeUtf "samples/h5dump_water_224.txt"
-- withFile outputFile WriteMode $ \handle -> do
--   hPutStrLn handle $ "HDF5 \"" <> inputFile <> "\" {"
--   hPutStrLn handle (graphToH5Dump graph)
--   hPutStrLn handle "}"
