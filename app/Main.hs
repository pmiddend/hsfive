module Main where

import Control.Monad (replicateM, void, when)
import Data.Binary (Get, getWord8)
import Data.Binary.Get (getByteString, getWord16le, getWord32le, getWord64le, runGet, runGetOrFail, skip)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Word (Word16, Word32, Word64, Word8)
import Debug.Trace (traceShowId)
import HsFive.Util
import System.File.OsPath (withBinaryFile)
import System.IO (Handle, IOMode (ReadMode), SeekMode (AbsoluteSeek, SeekFromEnd), hSeek, hTell)
import System.OsPath (encodeUtf)

data ObjectHeader = ObjectHeader
  { ohVersion :: !Word8,
    ohObjectReferenceCount :: !Word32,
    ohObjectHeaderSize :: !Word32,
    ohHeaderMessages :: ![BS.ByteString]
  }
  deriving (Show)

readKey :: Handle -> Address -> IO BSL.ByteString
readKey handle addr = do
  hSeek handle AbsoluteSeek (fromIntegral addr)
  strData <- BSL.hGet handle 200
  pure (BSL.takeWhile (/= 0) strData)

getObjectHeader :: Get ObjectHeader
getObjectHeader = do
  version <- getWord8
  skip 1
  messageCount <- getWord16le
  objectReferenceCount <- getWord32le
  objectHeaderSize <- getWord32le
  let readMessage = do
        messageType <- getWord16le
        headerMessageDataSize <- getWord16le
        flags <- getWord8
        skip 3
        getByteString (fromIntegral headerMessageDataSize)
  messages <- replicateM (fromIntegral messageCount) readMessage
  pure (ObjectHeader version objectReferenceCount objectHeaderSize messages)

data SymbolTableScratchpad
  = ScratchpadNoContent
  | ScratchpadObjectHeaderMetadata {h5stspBTreeAddress :: !Word64, h5stspNameHeapAddress :: !Word64}
  | ScratchpadSymbolicLinkMetadata {offsetToLinkValue :: !Word32}
  deriving (Show)

data SymbolTableEntry = SymbolTableEntry
  { h5steLinkNameOffset :: !Word64,
    h5steObjectHeaderAddress :: !Word64,
    h5steScratchpad :: !SymbolTableScratchpad
  }
  deriving (Show)

getSymbolTableEntry :: Get SymbolTableEntry
getSymbolTableEntry = do
  linkNameOffset <- getWord64le
  objectHeaderAddress <- getWord64le
  cacheType <- getWord32le
  void getWord32le
  case cacheType of
    0 -> do
      -- scratch pad is aways 16 bytes
      skip 16
      pure (SymbolTableEntry linkNameOffset objectHeaderAddress ScratchpadNoContent)
    1 -> do
      scratchPad <- ScratchpadObjectHeaderMetadata <$> getWord64le <*> getWord64le
      pure (SymbolTableEntry linkNameOffset objectHeaderAddress scratchPad)
    2 -> do
      scratchPad <- ScratchpadSymbolicLinkMetadata <$> getWord32le
      -- scratch pad is aways 16 bytes
      skip 12
      pure (SymbolTableEntry linkNameOffset objectHeaderAddress scratchPad)
    _ -> fail "invalid symbol table cache type"

data GroupSymbolTableNode = GroupSymbolTableNode
  { gstnVersion :: !Word8,
    gstnEntries :: ![SymbolTableEntry]
  }
  deriving (Show)

getGroupSymbolTableNode :: Get GroupSymbolTableNode
getGroupSymbolTableNode = do
  signature' <- getByteString 4
  -- "SNOD" in ASCII
  when (signature' /= BS.pack [83, 78, 79, 68]) (fail "invalid group symbol table node signature")
  version <- getWord8
  skip 1
  numberOfSymbols <- getWord16le
  entries <- replicateM (fromIntegral numberOfSymbols) getSymbolTableEntry
  pure (GroupSymbolTableNode version entries)

data BLinkTreeNodeTypeEnum = BLinkTreeNodeEnumGroup | BLinkTreeNodeEnumRawData deriving (Show)

getBLinkTreeNodeTypeEnum :: Get BLinkTreeNodeTypeEnum
getBLinkTreeNodeTypeEnum = do
  r <- getWord8
  case r of
    0 -> pure BLinkTreeNodeEnumGroup
    1 -> pure BLinkTreeNodeEnumRawData
    _ -> fail "invalid B-link node type"

type Length = Word64

type Address = Word64

data BLinkTreeNode = BLinkTreeNodeGroup
  { bltnNodeLevel :: !Word8,
    bltnEntriesUsed :: !Word16,
    bltnLeftSiblingAddress :: !(Maybe Word64),
    bltnRightSiblingAddress :: !(Maybe Word64),
    bltnKeyOffsets :: ![Length],
    bltnChildPointers :: ![Address]
  }
  deriving (Show)

getMaybeAddress :: Get (Maybe Word64)
getMaybeAddress = do
  a <- getWord64le
  pure $
    if a == 0xffffffffffffffff
      then Nothing
      else Just a

getLength :: Get Length
getLength = getWord64le

getAddress :: Get Address
getAddress = getWord64le

getBLinkTreeNode :: Get BLinkTreeNode
getBLinkTreeNode = do
  signature' <- getByteString 4
  -- "TREE" in ASCII
  when (signature' /= BS.pack [84, 82, 69, 69]) (fail "invalid B tree node signature")
  nodeType <- getBLinkTreeNodeTypeEnum
  case nodeType of
    BLinkTreeNodeEnumGroup -> do
      nodeLevel <- getWord8
      entriesUsed <- getWord16le
      leftSiblingAddress <- getMaybeAddress
      rightSiblingAddress <- getMaybeAddress
      keysAndPointers <- replicateM (fromIntegral (2 * entriesUsed + 1)) getLength
      pure
        ( BLinkTreeNodeGroup
            nodeLevel
            entriesUsed
            leftSiblingAddress
            rightSiblingAddress
            (dropEverySecond keysAndPointers)
            (dropEverySecond (drop 1 keysAndPointers))
        )

data Superblock = Superblock
  { h5sbVersionSuperblock :: !Word8,
    h5sbVersionFreeSpaceInfo :: !Word8,
    h5sbVersionRootGroupSymbolTableEntry :: !Word8,
    h5sbVersionSharedHeaderMessageFormat :: !Word8,
    h5sbOffsetSize :: !Word8,
    h5sbLengthSize :: !Word8,
    h5sbGroupLeafNodeK :: !Word16,
    h5sbGroupInternalNodeK :: !Word16,
    h5sbFileConsistencyFlags :: !Word32,
    h5sbBaseAddress :: !Word64,
    h5sbFileFreeSpaceInfoAddress :: !Word64,
    h5sbEndOfFileAddress :: !Word64,
    h5sbDriverInformationBlockAddress :: !Word64,
    h5sbSymbolTableEntry :: !SymbolTableEntry
  }
  deriving (Show)

superblockHeader :: BS.ByteString
superblockHeader = BS.pack [137, 72, 68, 70, 13, 10, 26, 10]

getSuperblock :: Get Superblock
getSuperblock = do
  magicBytes <- getByteString 8
  if magicBytes == superblockHeader
    then do
      superblockVersion <- getWord8
      if superblockVersion /= 0
        then fail "superblock version is not 0"
        else
          Superblock superblockVersion
            <$> getWord8
            <*> getWord8
            <*> (getWord8 *> getWord8)
            <*> getWord8
            <*> (getWord8 <* getWord8)
            <*> getWord16le
            <*> getWord16le
            <*> getWord32le
            <*> getWord64le
            <*> getWord64le
            <*> getWord64le
            <*> getWord64le
            <*> getSymbolTableEntry
    else fail "couldn't get magic 8 bytes"

readSuperblock' :: Handle -> Integer -> Integer -> IO (Maybe Superblock)
readSuperblock' handle fileSize start = do
  putStrLn $ "trying at " <> show start
  hSeek handle AbsoluteSeek start
  superblockCandidate <- BSL.hGet handle 200
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

data Heap = Heap
  { heapVersion :: !Word8,
    heapDataSegmentSize :: !Length,
    heapOffsetToHeadOfFreeList :: !Length,
    heapDataSegmentAddress :: !Address
  }
  deriving (Show)

getHeap :: Get Heap
getHeap = do
  signature' <- getByteString 4
  -- "HEAP" in ASCII
  when (signature' /= BS.pack [72, 69, 65, 80]) (fail "invalid heap signature")
  Heap <$> (getWord8 <* getWord16le <* getWord8) <*> getLength <*> getLength <*> getAddress

recurseIntoSymbolTableEntry :: Handle -> Int -> Maybe Address -> SymbolTableEntry -> IO ()
recurseIntoSymbolTableEntry handle depth maybeHeapAddress e =
  let prefix = replicate depth ' ' <> " | "
      printWithPrefix :: (Show a) => a -> IO ()
      printWithPrefix = putStrLn . (prefix <>) . show
      putStrLnWithPrefix x = putStrLn (prefix <> x)
   in do
        putStrLn (prefix <> "start recursion for " <> show e)
        case h5steScratchpad e of
          ScratchpadSymbolicLinkMetadata offsetToLinkValue' ->
            putStrLnWithPrefix "symbolic metadata"
          ScratchpadNoContent ->
            case maybeHeapAddress of
              Nothing -> putStrLnWithPrefix "have no heap address, cannot resolve stuff inside this group"
              Just heapAddress -> do
                k <- readKey handle (fromIntegral (heapAddress + h5steLinkNameOffset e))
                printWithPrefix k
          ScratchpadObjectHeaderMetadata btreeAddress heapAddress -> do
            hSeek handle AbsoluteSeek (fromIntegral btreeAddress)
            blinkTreenode <- BSL.hGet handle 200
            case runGetOrFail getBLinkTreeNode blinkTreenode of
              Left _ -> error (prefix <> "error reading b-link node")
              Right (_, _, node@(BLinkTreeNodeGroup {})) -> do
                printWithPrefix node

                hSeek handle AbsoluteSeek (fromIntegral heapAddress)
                heapData <- BSL.hGet handle 200
                case runGetOrFail getHeap heapData of
                  Left _ -> error "error reading heap"
                  Right (_, _, heap) -> do
                    let keyAddressesOnHeap :: [Address]
                        keyAddressesOnHeap = (\len -> heapDataSegmentAddress heap + len) <$> bltnKeyOffsets node
                        childAddressesOnHeap = bltnChildPointers node
                        readChild :: Address -> IO GroupSymbolTableNode
                        readChild addr = do
                          hSeek handle AbsoluteSeek (fromIntegral addr)
                          rawData <- BSL.hGet handle 200
                          pure (runGet getGroupSymbolTableNode rawData)
                    keysOnHeap <- mapM (readKey handle) keyAddressesOnHeap
                    childrenOnHeap <- mapM readChild childAddressesOnHeap
                    printWithPrefix heap
                    printWithPrefix keysOnHeap
                    printWithPrefix childrenOnHeap

                    mapM_ (recurseIntoSymbolTableEntry handle (depth + 2) (Just (heapDataSegmentAddress heap))) (concatMap gstnEntries childrenOnHeap)

main :: IO ()
main = do
  fileNameEncoded <- encodeUtf "/home/pmidden/178_data-00000.nx5"
  withBinaryFile fileNameEncoded ReadMode $ \handle -> do
    putStrLn "reading file"
    superblock <- readSuperblock handle

    case superblock of
      Nothing -> error "couldn't read superblock"
      Just superblock' -> do
        print superblock

        recurseIntoSymbolTableEntry handle 0 Nothing (h5sbSymbolTableEntry superblock')
