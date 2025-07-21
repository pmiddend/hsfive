{-# LANGUAGE BinaryLiterals #-}
{-# HLINT ignore "Use newtype instead of data" #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

module Main where

import Data.Binary.Get (runGetOrFail)
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Text.IO as TIO
import HsFive.CoreTypes
import HsFive.H5Dump (h5dump)
import HsFive.Types (readH5)
import System.Environment (getArgs)
import System.IO (Handle, SeekMode (AbsoluteSeek, SeekFromEnd), hSeek, hTell)
import System.OsPath (encodeUtf)

trace :: String -> a -> a
trace _x f = f

traceWith :: (a -> String) -> a -> a
traceWith _x f = f

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
  -- Commented out because this is an example where the first 12 bytes aren't layouted as H5 wants it, so it's "useless"
  -- let inputBytes = BS.pack [0, 0, 0, 9, 128, 210, 208, 222, 157, 64, 255, 223, 0, 114, 108, 100, 0]
  --     decompressed = bshufDecompressLz4 inputBytes 12 1 0

  -- case decompressed of
  --   DecompressError _ -> putStrLn "error decompressing"
  --   DecompressSuccess bytes numberBytes ->
  --     putStrLn ("unpacked " <> show numberBytes <> " bytes: " <> show (BS.unpack bytes))

  args <- getArgs

  case args of
    [inputFile, dumpFile] -> do
      fileNameEncoded <- encodeUtf inputFile
      rootGroup <- readH5 fileNameEncoded
      putStrLn $ "root node: " <> show rootGroup

      TIO.writeFile dumpFile (h5dump rootGroup)

    -- let imageNode = goToNode rootGroup (singletonPath "entry_0000" </ "0_measurement" </ "images")
    -- putStrLn $ "node: " <> show imageNode

    -- case imageNode of
    --   Nothing -> error "image node not found"
    --   Just (DatasetNode (DatasetData {datasetStorageLayout = LayoutChunked {layoutChunkedBTreeAddress, layoutChunkedSizes}, datasetDimensions, datasetDatatype, datasetFilters})) ->
    --     withBinaryFile fileNameEncoded ReadMode $ \handle -> do
    --       chunks <- readChunkInfos handle layoutChunkedBTreeAddress datasetDimensions
    --       putStrLn $ "chunks: " <> show chunks
    --       case chunks of
    --         (firstChunk : _) ->
    --           readSingleChunk
    --             handle
    --             datasetDatatype
    --             (fromIntegral (product layoutChunkedSizes))
    --             datasetFilters
    --             datasetDimensions
    --             firstChunk
    _ -> putStrLn "usage: Main <h5-file> <hdf5dump-file>"

-- putStrLn $ unpack $ h5dump rootGroup

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
