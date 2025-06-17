{-# LANGUAGE BinaryLiterals #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use newtype instead of data" #-}
module HsFive.CoreTypes where

import Control.Monad (replicateM, void, when)
import Data.Binary (Get, getWord8)
import Data.Binary.Get (Get, bytesRead, getByteString, getRemainingLazyByteString, getWord16le, getWord32le, getWord64le, isEmpty, isolate, runGet, runGetOrFail, skip)
import Data.Bits (shiftR, (.&.))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Foldable (forM_)
import Data.Word (Word16, Word32, Word64, Word8)
import HsFive.Util
import System.File.OsPath (withBinaryFile)
import System.IO (Handle, IOMode (ReadMode), SeekMode (AbsoluteSeek, SeekFromEnd), hSeek, hTell)
import System.OsPath (OsPath, encodeUtf)

data DataspaceDimension = DataspaceDimension
  { ddSize :: !Length,
    ddMaxSize :: !(Maybe Length)
  }
  deriving (Show)

data DatatypeClass
  = ClassFixedPoint
  | ClassFloatingPoint
  | ClassTime
  | ClassString
  | ClassBitField
  | ClassOpaque
  | ClassCompound
  | ClassReference
  | ClassEnumerated
  | ClassVariableLength
  | ClassArray
  | ClassComplex
  deriving (Show)

data VariableLengthStringPadding = PaddingNullTerminate | PaddingNull | PaddingSpace deriving (Show)

data VariableLengthStringCharacterSet = CharacterSetAscii | CharacterSetUtf8 deriving (Show)

data Datatype
  = DatatypeFixedPoint
  | DatatypeVariableLengthSequence
  | DatatypeVariableLengthString !VariableLengthStringPadding !VariableLengthStringCharacterSet !Word32
  deriving (Show)

data DataStorageSpaceAllocationTime
  = -- | Early allocation. Storage space for the entire dataset should
    -- be allocated in the file when the dataset is created.
    EarlyAllocation
  | -- | Late allocation. Storage space for the entire dataset should not be allocated until the dataset is written to.
    LateAllocation
  | -- | Incremental allocation. Storage space for the dataset should not be allocated until the portion of the dataset is written to. This is currently used in conjunction with chunked data storage for datasets.
    IncrementalAllocation
  deriving (Show)

data DataStorageFillValueWriteTime
  = -- | On allocation. The fill value is always written to the raw data storage when the storage space is allocated.
    OnAllocation
  | -- | Never. The fill value should never be written to the raw data storage.
    Never
  | -- | Fill value written if set by user. The fill value will be written to the raw data storage when the storage space is allocated only if the user explicitly set the fill value. If the fill value is the library default or is undefined, it will not be written to the raw data storage.
    IfSetByUser
  deriving (Show)

data DataStoragePipelineFilter = DataStoragePipelineFilter
  { dataStoragePipelineFilterId :: !Word16,
    dataStoragePipelineFilterName :: !(Maybe BS.ByteString),
    dataStoragePipelineFilterOptional :: !Bool,
    dataStoragePipelineFilterClientDataValues :: ![Word32]
  }
  deriving (Show)

data DataStorageLayoutClass
  = LayoutClassCompact
  | LayoutClassContiguous
  | LayoutClassChunked
  deriving (Show)

data DataStorageLayout
  = LayoutContiguousOld
      { layoutContiguousOldRawDataAddress :: !Word32,
        layoutContiguousOldSizes :: ![Word32]
      }
  | LayoutContiguous
      { layoutContiguousRawDataAddress :: !(Maybe Address),
        layoutContiguousSize :: !Length
      }
  | LayoutChunked
      { layoutChunkedBTreeAddress :: !(Maybe Address),
        layoutChunkedSizes :: ![Word32],
        layoutChunkedDatasetElementSize :: !Word32
      }
  | LayoutCompactOld {layoutCompactOldSizes :: ![Word32], layoutCompactOldDataSize :: !Word32}
  | LayoutCompact {layoutCompactSize :: !Word16}
  deriving (Show)

getDataspaceDimension :: Bool -> Get DataspaceDimension
getDataspaceDimension True = DataspaceDimension <$> getLength <*> getMaybeLength
getDataspaceDimension False = DataspaceDimension <$> getLength <*> pure Nothing

data DataspaceMessageData = DataspaceMessageData
  { dataspaceDimensions :: ![DataspaceDimension],
    dataspacePermutationIndices :: ![Length]
  }
  deriving (Show)

data DatatypeMessageData = DatatypeMessageData
  { datatypeMessageVersion :: !Word8,
    datatypeClass :: !Datatype
  }
  deriving (Show)

data DataStorageFilterPipelineMessageData
  = DataStorageFilterPipelineMessageData {dataStorageFilterPipelineFilters :: ![DataStoragePipelineFilter]}
  deriving (Show)

data Message
  = NilMessage
  | DataspaceMessage !DataspaceMessageData
  | SymbolTableMessage
      { symbolTableMessageV1BTreeAddress :: !Address,
        symbolTableMessageLocalHeapAddress :: !Address
      }
  | ObjectHeaderContinuationMessage {objectHeaderContinuationMessageOffset :: !Address, objectHeaderContinuationMessageLength :: !Length}
  | DatatypeMessage !DatatypeMessageData
  | DataStorageFillValueMessage
  | -- | This message describes the filter pipeline which should be applied to the data stream by providing filter identification numbers, flags, a name, and client data. This message may be present in the object headers of both dataset and group objects. For datasets, it specifies the filters to apply to raw data. For groups, it specifies the filters to apply to the group’s fractal heap. Currently, only datasets using chunked data storage use the filter pipeline on their raw data.
    DataStorageFilterPipelineMessage !DataStorageFilterPipelineMessageData
  | -- | The Data Layout message describes how the elements of a multi-dimensional array are stored in the HDF5 file.
    DataStorageLayoutMessage !DataStorageLayout
  | -- | The object modification time is a timestamp which indicates the time of the last modification of an object. The time is updated when any object header message changes according to the system clock where the change was posted.
    ObjectModificationTimeMessage {objectModificationTime :: !Word32}
  | AttributeMessage {attributeName :: BS.ByteString}
  deriving (Show)

data ObjectHeader = ObjectHeader
  { ohVersion :: !Word8,
    ohObjectReferenceCount :: !Word32,
    ohObjectHeaderSize :: !Word32,
    ohHeaderMessages :: ![Message]
    -- ohHeaderMessages :: ![(Word16, BS.ByteString)]
  }
  deriving (Show)

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

data GroupSymbolTableNode = GroupSymbolTableNode
  { gstnVersion :: !Word8,
    gstnEntries :: ![SymbolTableEntry]
  }
  deriving (Show)

data BLinkTreeNodeTypeEnum = BLinkTreeNodeEnumGroup | BLinkTreeNodeEnumRawData deriving (Show)

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
    h5sbBaseAddress :: !Address,
    h5sbFileFreeSpaceInfoAddress :: !(Maybe Address),
    h5sbEndOfFileAddress :: !Address,
    h5sbDriverInformationBlockAddress :: !(Maybe Address),
    h5sbSymbolTableEntry :: !SymbolTableEntry
  }
  deriving (Show)

data HeapHeader = HeapHeader
  { heapVersion :: !Word8,
    heapDataSegmentSize :: !Length,
    heapOffsetToHeadOfFreeList :: !(Maybe Length),
    heapDataSegmentAddress :: !Address
  }
  deriving (Show)

data HeapWithData = HeapWithData
  { heapHeader :: HeapHeader,
    heapData :: BSL.ByteString
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
    n -> fail ("invalid symbol table cache type: " <> show n)

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

getBLinkTreeNodeTypeEnum :: Get BLinkTreeNodeTypeEnum
getBLinkTreeNodeTypeEnum = do
  r <- getWord8
  case r of
    0 -> pure BLinkTreeNodeEnumGroup
    1 -> pure BLinkTreeNodeEnumRawData
    _ -> fail "invalid B-link node type"

getMaybeAddress :: Get (Maybe Word64)
getMaybeAddress = do
  a <- getWord64le
  pure $
    if a == 0xffffffffffffffff
      then Nothing
      else Just a

-- This is very weird. Addresses and lengths are different in the spec
-- (they can have different sizes), but in the definition of the level
-- 1 "heap" we read:
--
--   This is the offset within the heap data segment of the first free
--   block (or the undefined address if there is no no free block).
--
-- So we assume that addresses and lengths are of the same size and
-- can be treated the same way.
getMaybeLength :: Get (Maybe Word64)
getMaybeLength = do
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

superblockHeader :: BS.ByteString
superblockHeader = BS.pack [137, 72, 68, 70, 13, 10, 26, 10]

readKey :: Handle -> Address -> IO BSL.ByteString
readKey handle addr = do
  hSeek handle AbsoluteSeek (fromIntegral addr)
  strData <- BSL.hGet handle 200
  pure (BSL.takeWhile (/= 0) strData)

getDatatypeClass :: Word8 -> Get DatatypeClass
getDatatypeClass 0 = pure ClassFixedPoint
getDatatypeClass 1 = pure ClassFloatingPoint
getDatatypeClass 2 = pure ClassTime
getDatatypeClass 3 = pure ClassString
getDatatypeClass 4 = pure ClassBitField
getDatatypeClass 5 = pure ClassOpaque
getDatatypeClass 6 = pure ClassCompound
getDatatypeClass 7 = pure ClassReference
getDatatypeClass 8 = pure ClassEnumerated
getDatatypeClass 9 = pure ClassVariableLength
getDatatypeClass 10 = pure ClassArray
getDatatypeClass 11 = pure ClassComplex
getDatatypeClass n = fail ("invalid datatype class " <> show n)

getDataStorageFillValueWriteTime :: Get DataStorageFillValueWriteTime
getDataStorageFillValueWriteTime = do
  s <- getWord8
  case s of
    0 -> pure OnAllocation
    1 -> pure Never
    2 -> pure IfSetByUser
    n -> fail ("invalid data storage fill value write time value " <> show n)

getDataStorageSpaceAllocationTime :: Get DataStorageSpaceAllocationTime
getDataStorageSpaceAllocationTime = do
  s <- getWord8
  case s of
    1 -> pure EarlyAllocation
    2 -> pure LateAllocation
    3 -> pure IncrementalAllocation
    n -> fail ("invalid data storage space allocation time value " <> show n)

getDataStorageLayoutClass :: Get DataStorageLayoutClass
getDataStorageLayoutClass = do
  s <- getWord8
  case s of
    0 -> pure LayoutClassCompact
    1 -> pure LayoutClassContiguous
    2 -> pure LayoutClassChunked
    n -> fail ("invalid data storage layout class value " <> show n)

getMessage :: Word16 -> Get Message
getMessage 0x0000 = do
  void getRemainingLazyByteString
  pure NilMessage
-- The dataspace message describes the number of dimensions (in other
-- words, “rank”) and size of each dimension that the data object has.
-- This message is only used for datasets which have a simple,
-- rectilinear, array-like layout; datasets requiring a more complex
-- layout are not yet supported.
getMessage 0x0001 = do
  -- This value is used to determine the format of the Dataspace
  -- Message. When the format of the information in the message is
  -- changed, the version number is incremented and can be used to
  -- determine how the information in the object header is formatted.
  -- This document describes version one (1) (there was no version
  -- zero (0)).
  version <- getWord8
  when (version /= 1) (fail ("dataspace version is not 1 but " <> show version))
  -- This value is the number of dimensions that the data object has.
  dimensionality <- getWord8
  -- This field is used to store flags to indicate the presence of
  -- parts of this message. Bit 0 (the least significant bit) is used
  -- to indicate that maximum dimensions are present. Bit 1 is used to
  -- indicate that permutation indices are present.
  flags <- getWord8
  let maxDimsStored :: Bool
      maxDimsStored = flags .&. 1 > 0
      permutationIndicesStored = flags .&. 2 > 0
  -- Reserved
  skip 5
  if dimensionality == 0
    then pure (DataspaceMessage (DataspaceMessageData [] []))
    else do
      dimensions <- replicateM (fromIntegral dimensionality) getLength
      maxSizes <-
        if maxDimsStored
          then replicateM (fromIntegral dimensionality) getMaybeLength
          else pure (replicate (fromIntegral dimensionality) Nothing)
      permutationIndices <-
        if permutationIndicesStored
          then replicateM (fromIntegral dimensionality) getLength
          else pure []
      -- TODO: This message has _lots_ more information to it
      pure (DataspaceMessage (DataspaceMessageData (zipWith DataspaceDimension dimensions maxSizes) permutationIndices))
-- Explanation of "datatype" in general
--
-- https://support.hdfgroup.org/documentation/hdf5/latest/_l_b_datatypes.html
getMessage 0x003 = do
  classAndVersion <- getWord8
  let classNumeric = classAndVersion .&. 0b1111
      version = (classAndVersion .&. 0b11110000) `shiftR` 4
  class' <- getDatatypeClass classNumeric
  bits0to7 <- getWord8
  bits8to15 <- getWord8
  bits16to23 <- getWord8
  -- The size of a datatype element in bytes.
  size <- getWord32le
  case class' of
    ClassFixedPoint -> do
      skip 8
      pure (DatatypeMessage (DatatypeMessageData version DatatypeFixedPoint))
    ClassVariableLength -> do
      let variableType = bits0to7 .&. 0b1111
      case variableType of
        0 -> pure (DatatypeMessage (DatatypeMessageData version DatatypeVariableLengthSequence))
        1 -> do
          let paddingTypeNumeric = (bits0to7 .&. 0b11110000) `shiftR` 4
          paddingType <- if paddingTypeNumeric == 0 then pure PaddingNullTerminate else if paddingTypeNumeric == 1 then pure PaddingNull else if paddingTypeNumeric == 2 then pure PaddingSpace else fail ("invalid variable length string padding type " <> show paddingTypeNumeric)
          let characterSetNumeric = bits8to15 .&. 0b1111
          characterSet <- if characterSetNumeric == 0 then pure CharacterSetAscii else if characterSetNumeric == 1 then pure CharacterSetUtf8 else fail ("invalid variable length string character set " <> show characterSetNumeric)
          -- for now, skip the content (the size)
          skip (fromIntegral size)
          pure (DatatypeMessage (DatatypeMessageData version (DatatypeVariableLengthString paddingType characterSet size)))
        _ -> fail $ "variable length which is neither sequence nor string, bits are: " <> show variableType
    _ -> fail ("class " <> show class' <> " properties not supported yet")
getMessage 0x0005 = do
  version <- getWord8
  case version of
    1 -> do
      spaceAllocationTime <- getDataStorageSpaceAllocationTime
      fillValueWriteTime <- getDataStorageFillValueWriteTime
      fillValueDefined <- getWord8
      size <- getWord32le
      -- TODO: the actual fill value depends on the datatype for the
      -- dataset - this must be a parameter to this function then
      _fillValue <- getByteString (fromIntegral size)
      pure DataStorageFillValueMessage
    2 -> do
      spaceAllocationTime <- getDataStorageSpaceAllocationTime
      fillValueWriteTime <- getDataStorageFillValueWriteTime
      fillValueDefined <- getWord8
      case fillValueDefined of
        -- No need to read the fill value and its size, since it's not defined
        0 -> pure DataStorageFillValueMessage
        _ -> do
          size <- getWord32le
          -- TODO: the actual fill value depends on the datatype for the
          -- dataset - this must be a parameter to this function then
          _fillValue <- getByteString (fromIntegral size)
          pure DataStorageFillValueMessage
    3 -> fail "version 3 of data storage fill value not supported yet (table in spec is weird)"
    n -> fail ("invalid version of data storage fill value message " <> show n)
getMessage 0x000b = do
  version <- getWord8
  when (version /= 1) (fail ("data storage filter pipeline message has invalid version " <> show version))
  -- The total number of filters described in this message. The maximum possible number of filters in a message is 32.
  numberOfFilters <- getWord8
  -- All reserved
  skip 6
  filters' <- replicateM (fromIntegral numberOfFilters) $ do
    -- Description too long, abbreviated: This value, often referred to
    -- as a filter identifier, is designed to be a unique identifier
    -- for the filter. Values from zero through 32,767 are reserved
    -- for filters supported by The HDF Group in the HDF5 library and
    -- for filters requested and supported by third parties.
    --
    -- Values from 32768 to 65535 are reserved for non-distributed
    -- uses (for example, internal company usage) or for application
    -- usage when testing a feature. The HDF Group does not track or
    -- document the use of the filters with identifiers from this
    -- range.
    filterIdentification <- getWord16le
    nameLength <- getWord16le
    flags <- getWord16le
    numberOfValuesForClientData <- getWord16le
    name <- if nameLength == 0 then pure Nothing else Just <$> getByteString (fromIntegral nameLength)
    -- Note that the spec is vague about this possibly being _signed_ integers?
    clientData <- replicateM (fromIntegral numberOfValuesForClientData) getWord32le
    -- 	Four bytes of zeroes are added to the message at this point if the Client Data Number of Values field contains an odd number.
    when (numberOfValuesForClientData `mod` 2 == 1) (skip 4)
    pure (DataStoragePipelineFilter filterIdentification name (flags .&. 1 == 1) clientData)
  pure (DataStorageFilterPipelineMessage (DataStorageFilterPipelineMessageData filters'))
getMessage 0x0008 = do
  version <- getWord8
  case version of
    3 -> do
      layoutClass <- getDataStorageLayoutClass

      case layoutClass of
        -- Order of appearance in the spec
        LayoutClassCompact -> do
          -- (Note: The dimensionality information is in the Dataspace message)
          -- This field contains the size of the raw data for the
          -- dataset array, in bytes.
          size <- getWord16le

          -- This field contains the raw data for the dataset array.
          -- TODO: interpretation of this data needs type information
          void $ getByteString (fromIntegral size)

          pure (DataStorageLayoutMessage (LayoutCompact size))
        LayoutClassContiguous -> do
          -- This is the address of the raw data in the file. The
          -- address may have the undefined address value, to indicate
          -- that storage has not yet been allocated for this array.
          rawDataAddress <- getMaybeAddress
          size <- getLength
          pure (DataStorageLayoutMessage (LayoutContiguous rawDataAddress size))
        LayoutClassChunked -> do
          -- A chunk has a fixed dimensionality. This field specifies
          -- the number of dimension size fields later in the message.
          dimensionality <- getWord8
          -- skip 3
          -- This is the address of the III.A.1. Disk Format: Level
          -- 1A1 - Version 1 B-trees that is used to look up the
          -- addresses of the chunks that actually store portions of
          -- the array data.
          address <- getMaybeAddress
          -- These values define the dimension size of a single chunk,
          -- in units of array elements (not bytes). The first
          -- dimension stored in the list of dimensions is the slowest
          -- changing dimension and the last dimension stored is the
          -- fastest changing dimension.
          sizes <- replicateM (fromIntegral dimensionality) getWord32le
          datasetElementSize <- getWord32le
          -- The need to skip this is _very_ weird. Not sure why we need it, it's not in the spec.
          skip 1
          pure (DataStorageLayoutMessage (LayoutChunked address sizes datasetElementSize))
    _ -> do
      when (version /= 1 && version /= 2) (fail ("version of the data layout message is not supported yet: " <> show version))
      dimensionality <- getWord8
      layoutClass <- getDataStorageLayoutClass
      -- Reserved
      skip 5
      case layoutClass of
        LayoutClassContiguous -> do
          rawDataAddress <- getWord32le
          sizes <- replicateM (fromIntegral dimensionality) getWord32le
          pure (DataStorageLayoutMessage (LayoutContiguousOld rawDataAddress sizes))
        LayoutClassChunked -> do
          bTreeAddress <- getMaybeAddress
          sizes <- replicateM (fromIntegral dimensionality) getWord32le
          datasetElementSize <- getWord32le
          pure (DataStorageLayoutMessage (LayoutChunked bTreeAddress sizes datasetElementSize))
        LayoutClassCompact -> do
          sizes <- replicateM (fromIntegral dimensionality) getWord32le
          compactDataSize <- getWord32le
          -- TODO: We need the datatype size to read the actual compact data
          pure (DataStorageLayoutMessage (LayoutCompactOld sizes compactDataSize))
getMessage 0x0010 = ObjectHeaderContinuationMessage <$> getAddress <*> getLength
getMessage 0x0011 = SymbolTableMessage <$> getAddress <*> getAddress
getMessage 0x0012 = do
  version <- getWord8
  when (version /= 1) (fail ("invalid object modification time version " <> show version))
  -- Reserved
  skip 3
  ObjectModificationTimeMessage <$> getWord32le
getMessage 0x000c = do
  version <- getWord8
  when (version /= 1) (fail ("invalid attribute message version " <> show version))
  -- Reserved
  skip 1
  nameSize <- getWord16le
  datatypeSize <- getWord16le
  dataspaceSize <- getWord16le
  name <- getByteString (fromIntegral nameSize)
  let skipTo8 s = do
        let r = s `mod` 8
        when (r > 0) (skip (fromIntegral (8 - r)))
  skipTo8 nameSize
  datatypeMessage <- getMessage 0x0003
  -- datatypeRaw <- getByteString (fromIntegral (trace ("name " <> show name) datatypeSize))
  -- skipTo8 (trace ("received data type message " <> show datatypeMessage) datatypeSize)
  -- dataspaceRaw <- getByteString (fromIntegral (trace ("skipping over dataspace " <> show dataspaceSize) dataspaceSize))
  dataspaceMessage <- getMessage 0x0001
  skipTo8 dataspaceSize
  remainder <- getRemainingLazyByteString
  pure (AttributeMessage name)
getMessage n = fail ("invalid message type " <> show n)

getObjectHeaderV1 :: Get ObjectHeader
getObjectHeaderV1 = do
  version <- getWord8
  when (version /= 1) (fail ("object header version was not 1 but " <> show version))
  skip 1
  -- This value determines the total number of messages listed in
  -- object headers for this object. This value includes the messages
  -- in continuation messages for this object.
  messageCount <- getWord16le
  -- This value specifies the number of “hard links” to this object
  -- within the current file. References to the object from external
  -- files, “soft links” in this file and object references in this
  -- file are not tracked.
  objectReferenceCount <- getWord32le
  -- This value specifies the number of bytes of header message data
  -- following this length field that contain object header messages
  -- for this object header. This value does not include the size of
  -- object header continuation blocks for this object elsewhere in
  -- the file.
  objectHeaderSize <- getWord32le
  skip 4
  -- The following code looks a little weird, but the problem is that
  -- "messageCount" above doesn't give us the number of messages
  -- \*only* in this object header. It could give us the number "4",
  -- but there is just one message here: a continuation message. So we
  -- isolate to the number of bytes we expect and read until we don't
  -- have any bytes anymore.
  let readMessage = do
        messageType <- getWord16le
        headerMessageDataSize <- getWord16le
        flags <- getWord8
        skip 3
        isolate (fromIntegral headerMessageDataSize) (getMessage messageType)
      -- decodeMessages 0 = pure []
      decodeMessages 0 = getRemainingLazyByteString >> pure []
      decodeMessages maxMessages = do
        bytesRead' <- bytesRead
        let remainder = bytesRead' `mod` 8
        skip (fromIntegral remainder)
        -- skip (fromIntegral remainder)
        empty <- isEmpty
        if empty
          then pure []
          else do
            m <- readMessage
            ms <- decodeMessages (maxMessages - 1)
            pure (m : ms)
  messages <-
    isolate
      (fromIntegral objectHeaderSize)
      -- (decodeMessages (trace ("message count: " <> show messageCount) messageCount))
      (decodeMessages messageCount)
  -- rawContent <- getByteString (fromIntegral headerMessageDataSize)
  -- pure (messageType, getMessage rawContent)
  -- messages <- replicateM (fromIntegral messageCount) readMessage
  pure (ObjectHeader version objectReferenceCount objectHeaderSize messages)

getHeapHeader :: Get HeapHeader
getHeapHeader = do
  signature' <- getByteString 4
  -- "HEAP" in ASCII
  when (signature' /= BS.pack [72, 69, 65, 80]) (fail "invalid heap signature")
  HeapHeader <$> (getWord8 <* getWord16le <* getWord8) <*> getLength <*> getMaybeLength <*> getAddress

getSuperblock :: Get Superblock
getSuperblock = do
  magicBytes <- getByteString 8
  if magicBytes == superblockHeader
    then do
      superblockVersion <- getWord8
      if superblockVersion /= 0
        then fail "superblock version is not 0"
        else do
          sb <-
            Superblock superblockVersion
              <$> getWord8
              <*> getWord8
              <*> (getWord8 *> getWord8)
              <*> getWord8
              <*> (getWord8 <* getWord8)
              <*> getWord16le
              <*> getWord16le
              <*> getWord32le
              <*> getAddress
              <*> getMaybeAddress
              <*> getAddress
              <*> getMaybeAddress
              <*> getSymbolTableEntry
          -- Fix this to 0 for now, but just out of extreme caution for our parsing algorithm
          when (h5sbVersionFreeSpaceInfo sb /= 0) $ do
            fail ("superblock free space info isn't 0 but " <> show (h5sbVersionFreeSpaceInfo sb))
          -- Fix this to 0 for now, but just out of extreme caution for our parsing algorithm
          when (h5sbVersionRootGroupSymbolTableEntry sb /= 0) $ do
            fail ("Version Number of the Root Group Symbol Table Entry isn't 0 but " <> show (h5sbVersionRootGroupSymbolTableEntry sb))
          -- Fix this to 0 for now, but just out of extreme caution for our parsing algorithm
          when (h5sbVersionSharedHeaderMessageFormat sb /= 0) $ do
            fail ("Version Number of the Shared Header Message Format isn't 0 but " <> show (h5sbVersionSharedHeaderMessageFormat sb))
          -- No real reason to constrain this, we should support different sizes here
          when (h5sbOffsetSize sb /= 8) $ do
            fail ("Size of Offsets isn't 8 but " <> show (h5sbOffsetSize sb))
          -- No real reason to constrain this, we should support different sizes here
          when (h5sbLengthSize sb /= 8) $ do
            fail ("Size of Lengths isn't 8 but " <> show (h5sbLengthSize sb))
          when (h5sbGroupLeafNodeK sb == 0) $ do
            fail ("Group Leaf Node K must be greater than zero, but is " <> show (h5sbGroupLeafNodeK sb))
          when (h5sbGroupInternalNodeK sb == 0) $ do
            fail ("Group Internal Node K must be greater than zero, but is " <> show (h5sbGroupInternalNodeK sb))
          pure sb
    else fail "couldn't get magic 8 bytes"
