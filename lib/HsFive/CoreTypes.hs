{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# HLINT ignore "Use <$>" #-}
{-# OPTIONS_GHC -Wno-type-defaults #-}
{-# HLINT ignore "Use newtype instead of data" #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

module HsFive.CoreTypes where

import Control.Applicative ((<|>))
import Control.Monad (replicateM, replicateM_, void, when)
import Data.Binary (Get, getWord8)
import Data.Binary.Get (bytesRead, getByteString, getLazyByteStringNul, getRemainingLazyByteString, getWord16be, getWord16le, getWord32be, getWord32le, getWord64be, getWord64le, isEmpty, isolate, label, skip)
import Data.Binary.IEEE754 (getFloat32be, getFloat64le)
import Data.Bits (Bits (shiftL, (.|.)), shiftR, (.&.))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Maybe (fromJust)
import Data.Text.Encoding (decodeLatin1)
import Data.Text.Read (decimal)
import Data.Time (secondsToNominalDiffTime)
import Data.Time.Clock (UTCTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Data.Word (Word16, Word32, Word64, Word8)
import Debug.Trace (trace)
import HsFive.Util
import System.IO (Handle, SeekMode (AbsoluteSeek), hSeek)

debugLog :: (Show a) => String -> a -> a
debugLog str x = trace (str <> ": " <> show x) x

replicateM' :: (Applicative m, Integral a1) => a1 -> m a2 -> m [a2]
replicateM' n = replicateM (fromIntegral n)

getByteString' :: (Integral a) => a -> Get BS.ByteString
getByteString' = getByteString . fromIntegral

-- trace :: String -> a -> a
-- trace x f = f

-- traceWith :: (a -> String) -> a -> a
-- traceWith x f = f

data DataspaceDimension = DataspaceDimension
  { ddSize :: !Length,
    ddMaxSize :: !(Maybe Length)
  }
  deriving (Show)

data ByteOrder = LittleEndian | BigEndian deriving (Show, Eq)

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

data ReferenceType = ObjectReference | DatasetRegionReference deriving (Show)

data StringPadding = PaddingNullTerminate | PaddingNull | PaddingSpace deriving (Show)

data CharacterSet = CharacterSetAscii | CharacterSetUtf8 deriving (Show)

data MantissaNormalization
  = NoNormalization
  | MostSignificantMantissaAlwaysSet
  | MostSignificantMantissaImplied
  deriving (Show, Eq)

type EnumerationMap = [(BSL.ByteString, Int)]

data Datatype
  = DatatypeFixedPoint
      { fixedPointDataElementSize :: !Word32,
        fixedPointByteOrder :: !ByteOrder,
        fixedPointLoPadBit :: !Bool,
        fixedPointHiPadBit :: !Bool,
        fixedPointSigned :: !Bool,
        fixedPointBitOffset :: !Word16,
        fixedPointBitPrecision :: !Word16
      }
  | DatatypeVariableLengthSequence
  | DatatypeVariableLengthString !StringPadding !CharacterSet !Word32
  | DatatypeString !StringPadding !CharacterSet !Word32
  | DatatypeFloatingPoint
      { floatingPointByteOrder :: !ByteOrder,
        floatingPointLowBitPad :: !Bool,
        floatingPointHighBitPad :: !Bool,
        floatingPointInternalBitPad :: !Bool,
        floatingPointMantissaNormalization :: !MantissaNormalization,
        floatingPointSignLocation :: !Int,
        floatingPointBitOffset :: !Word16,
        floatingPointBitPrecision :: !Word16,
        floatingPointExponentLocation :: !Word8,
        floatingPointExponentSize :: !Word8,
        floatingPointMantissaLocation :: !Word8,
        floatingPointMantissaSize :: !Word8,
        floatingPointExponentBias :: !Word32
      }
  | DatatypeCompoundV1 ![CompoundDatatypeMemberV1]
  | DatatypeCompoundV2 ![CompoundDatatypeMemberV2]
  | DatatypeArray {arraySizes :: ![Word32], arrayBaseType :: !Datatype}
  | DatatypeReference !ReferenceType
  | DatatypeOpaqaue !BS.ByteString
  | DatatypeEnumeration !Datatype !EnumerationMap
  deriving (Show)

data AttributeContentCompoundMember = AttributeContentCompoundMember
  { attributeContentCompoundMemberName :: !BS.ByteString,
    attributeContentCompoundMemberType :: !AttributeContent
  }
  deriving (Show)

data AttributeContent
  = AttributeContentFixedString !BS.ByteString
  | AttributeContentVariableString !Address !Word32 !Word32 !CharacterSet
  | AttributeContentIntegral !Integer
  | AttributeContentFloating !Double
  | AttributeContentReference ReferenceType !BSL.ByteString
  | AttributeContentEnumeration !EnumerationMap !Int
  | AttributeContentCompound ![AttributeContentCompoundMember]
  | AttributeContentTodo !Datatype !BSL.ByteString
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
  | LayoutClassVirtual
  deriving (Show)

data LayoutChunkedV4Flag = DoNotApplyFilterToPartialEdgeChunk | SingleIndexWithFilter deriving (Show)

data LayoutChunkedV4IndexingInformation
  = LayoutChunkedV4IndexingSingleChunk
      { layoutV4SingleChunkSize :: !Word32,
        layoutV4SingleChunkFilters :: !Word32
      }
  | LayoutChunkedV4IndexingImplicit
  | LayoutChunkedV4IndexingFixedArray {layoutV4FixedArrayPageBits :: !Word8}
  | LayoutChunkedV4IndexingExtensibleArray
      { layoutV4ExtensibleArrayMaxBits :: !Word8,
        layoutV4ExtensibleArrayIndexElements :: !Word8,
        layoutV4ExtensibleArrayMinPointers :: !Word8,
        layoutV4ExtensibleArrayMinElements :: !Word8,
        layoutV4ExtensibleArrayPageBits :: !Word8
      }
  | LayoutChunkedV4IndexingV2BTree
      { layoutV4B2TreeNodeSize :: !Word32,
        layoutV4B2TreeSplitPercent :: !Word8,
        layoutV4B2TreeMergePercent :: !Word8
      }
  deriving (Show)

data DataStorageLayout
  = LayoutContiguousOld
      { layoutContiguousOldRawDataAddress :: !Address,
        layoutContiguousOldSizes :: ![Word32]
      }
  | LayoutContiguous
      { layoutContiguousRawDataAddress :: !(Maybe Address),
        layoutContiguousSize :: !Length
      }
  | LayoutChunked
      { layoutChunkedBTreeAddress :: !Address,
        layoutChunkedSizes :: ![Word32],
        layoutChunkedDatasetElementSize :: !Word32
      }
  | LayoutChunkedV4
      { layoutChunkedV4Sizes :: ![Word32],
        layoutChunkedV4Flags :: ![LayoutChunkedV4Flag],
        layoutChunkedIndexingInformation :: !LayoutChunkedV4IndexingInformation
      }
  | LayoutVirtualStorage {layoutVirtualStorageAddress :: !Address, layoutVirtualStorageIndex :: !Word32}
  | LayoutCompactOld {layoutCompactOldSizes :: ![Word32], layoutCompactOldDataSize :: !Word32}
  | LayoutCompact {layoutCompactSize :: !Word16}
  deriving (Show)

getDataspaceDimension :: Bool -> Get DataspaceDimension
getDataspaceDimension True = DataspaceDimension <$> getLength <*> getMaybeLength
getDataspaceDimension False = DataspaceDimension <$> getLength <*> pure Nothing

data GlobalHeapObject = GlobalHeapObject
  { globalHeapObjectIndex :: !Word16,
    globalHeapObjectRefCount :: !Word16,
    globalHeapObjectData :: !BS.ByteString
  }
  deriving (Show)

data GlobalHeap = GlobalHeap
  { globalHeapVersion :: !Word8,
    globalHeapObjects :: ![GlobalHeapObject]
  }
  deriving (Show)

data DataspaceType
  = DataspaceScalar
  | DataspaceSimple
  | DataspaceNull
  deriving (Show)

data DataspaceMessageData = DataspaceMessageData
  { dataspaceDimensions :: ![DataspaceDimension],
    dataspaceType :: !(Maybe DataspaceType),
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

data AttributeData = AttributeData
  { attributeName :: !BS.ByteString,
    attributeDatatypeMessageData :: !DatatypeMessageData,
    attributeDataspaceMessageData :: !DataspaceMessageData,
    attributeContent :: !AttributeContent
  }
  deriving (Show)

data SymbolTableMessageData = SymbolTableMessageData
  { symbolTableMessageV1BTreeAddress :: !Address,
    symbolTableMessageLocalHeapAddress :: !Address
  }
  deriving (Show)

data LinkInfoMessageData = LinkInfoMessageData
  { linkInfoMaximumCreationIndex :: !(Maybe Word64),
    linkInfoFractalHeapAddress :: !(Maybe Address),
    linkInfoBtreeAddress :: !(Maybe Address),
    linkInfoBtreeCreationOrderIndex :: !(Maybe Word64)
  }
  deriving (Show)

data AttributeInfoMessageData = AttributeInfoMessageData
  { attributeInfoMaximumCreationIndex :: !(Maybe Word16),
    attributeInfoFractalHeapAddress :: !Address,
    attributeInfoAttributeNamev2BtreeAddress :: !Address,
    attributeInfoCreationOrderv2BtreeAddress :: !(Maybe Address)
  }
  deriving (Show)

data LinkMessageData = LinkMessageData
  { linkInfoCreationOrder :: !(Maybe Word64),
    linkInfoLinkNameCharacterSet :: !(Maybe Word8),
    linkInfoLinkName :: !BS.ByteString,
    linkInfoLinkType :: !LinkType
  }
  deriving (Show)

data GroupInfoMessageData = GroupInfoMessageData
  { groupInfoMaxCompactValueLinkPhaseChange :: !(Maybe Word16),
    groupInfoMaxDenseValueLinkPhaseChange :: !(Maybe Word16),
    groupInfoEstimatedNumberOfEntries :: !(Maybe Word16),
    groupInfoEstimatedLinkNameLengthOfEntries :: !(Maybe Word16)
  }
  deriving (Show)

type FractalHeapId = Word64

data SharedMessageType
  = SharedMessageTypeShared !FractalHeapId
  | SharedMessageTypeCommitted !Address
  | SharedMessageShareable
  deriving (Show)

data SharedMessageData = SharedMessageData
  { sharedMessageOriginalType :: !Word16,
    sharedMessageType :: !SharedMessageType
  }
  deriving (Show)

data Message
  = NilMessage
  | DataspaceMessage !DataspaceMessageData
  | LinkInfoMessage !LinkInfoMessageData
  | LinkMessage !LinkMessageData
  | SymbolTableMessage !SymbolTableMessageData
  | SharedMessage !SharedMessageData
  | ObjectHeaderContinuationMessage {objectHeaderContinuationMessageOffset :: !Address, objectHeaderContinuationMessageLength :: !Length}
  | DatatypeMessage !DatatypeMessageData
  | -- Deliberately left blank for now
    DataStorageFillValueMessage
  | -- Deliberately left blank for now
    DataStorageFillValueOldMessage
  | -- | This message describes the filter pipeline which should be applied to the data stream by providing filter identification numbers, flags, a name, and client data. This message may be present in the object headers of both dataset and group objects. For datasets, it specifies the filters to apply to raw data. For groups, it specifies the filters to apply to the group’s fractal heap. Currently, only datasets using chunked data storage use the filter pipeline on their raw data.
    DataStorageFilterPipelineMessage !DataStorageFilterPipelineMessageData
  | -- | The Data Layout message describes how the elements of a multi-dimensional array are stored in the HDF5 file.
    DataStorageLayoutMessage !DataStorageLayout
  | -- | The object modification time is a timestamp which indicates the time of the last modification of an object. The time is updated when any object header message changes according to the system clock where the change was posted.
    ObjectModificationTimeMessage {objectModificationTime :: !Word32}
  | -- | The object modification date and time is a timestamp which indicates (using ISO-8601 date and time format) the last modification of an object. The time is updated when any object header message changes according to the system clock where the change was posted. All fields of this message should be interpreted as coordinated universal time (UTC).
    ObjectModificationTimeOldMessage
      { objectModificationTimeOldYear :: !Int,
        objectModificationTimeOldMonth :: !Int,
        objectModificationTimeOldDayOfMonth :: !Int,
        objectModificationTimeOldHour :: !Int,
        objectModificationTimeOldMinute :: !Int,
        objectModificationTimeOldSecond :: !Int
      }
  | AttributeMessage !AttributeData
  | AttributeInfoMessage !AttributeInfoMessageData
  | GroupInfoMessage !GroupInfoMessageData
  deriving (Show)

data ObjectHeaderTimes = ObjectHeaderTimes
  { ohAccessTime :: !UTCTime,
    ohModificationTime :: !UTCTime,
    ohChangeTime :: !UTCTime,
    ohBirthTime :: !UTCTime
  }
  deriving (Show)

data ObjectHeader = ObjectHeader
  { ohVersion :: !Word8,
    ohObjectReferenceCount :: !Word32,
    ohTimes :: !(Maybe ObjectHeaderTimes),
    ohHeaderMessages :: ![Message]
  }
  deriving (Show)

data SymbolTableScratchpad
  = ScratchpadNoContent
  | ScratchpadObjectHeaderMetadata {h5stspBTreeAddress :: !Word64, h5stspNameHeapAddress :: !Word64}
  | ScratchpadSymbolicLinkMetadata {offsetToLinkValue :: !Word32}
  deriving (Show)

data SymbolTableEntry a = SymbolTableEntry
  { h5steLinkNameOffset :: !Word64,
    h5steObjectHeaderAddress :: !a,
    h5steScratchpad :: !SymbolTableScratchpad
  }
  deriving (Show, Functor)

data GroupSymbolTableNode a = GroupSymbolTableNode
  { gstnVersion :: !Word8,
    gstnEntries :: ![SymbolTableEntry a]
  }
  deriving (Show)

data BLinkTreeNodeTypeEnum = BLinkTreeNodeEnumGroup | BLinkTreeNodeEnumRawData deriving (Show)

type Length = Word64

type Address = Word64

data ChunkInfo a = ChunkInfo
  { ciSize :: !Word32,
    ciFilterMask :: !Word32,
    ciChunkOffsets :: ![Length],
    ciChunkPointer :: !a
  }
  deriving (Show)

data BLinkTreeNode
  = BLinkTreeNodeGroup
      { bltnNodeLevel :: !Word8,
        bltnEntriesUsed :: !Word16,
        bltnLeftSiblingAddress :: !(Maybe Word64),
        bltnRightSiblingAddress :: !(Maybe Word64),
        bltnKeyOffsets :: ![Length],
        bltnChildPointers :: ![Address]
      }
  | BLinkTreeNodeChunkedRawData
      { bltnNodeLevel :: !Word8,
        bltnEntriesUsed :: !Word16,
        bltnLeftSiblingAddress :: !(Maybe Word64),
        bltnRightSiblingAddress :: !(Maybe Word64),
        bltnChunks :: ![ChunkInfo Address],
        bltnLastChunk :: !(ChunkInfo (Maybe Address))
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
    h5sbSymbolTableEntry :: !(SymbolTableEntry Address)
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

getSymbolTableEntry :: Get (SymbolTableEntry (Maybe Address))
getSymbolTableEntry = do
  linkNameOffset <- getWord64le
  objectHeaderAddress <- getMaybeAddress
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

getGroupSymbolTableNode :: Get (GroupSymbolTableNode (Maybe Address))
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

getBLinkTreeNode :: Maybe [DataspaceDimension] -> Get BLinkTreeNode
getBLinkTreeNode maybeDataspace = do
  signature' <- getByteString 4
  -- "TREE" in ASCII
  when (signature' /= BS.pack [84, 82, 69, 69]) (fail "invalid B tree node signature")
  nodeType <- getBLinkTreeNodeTypeEnum
  nodeLevel <- getWord8
  entriesUsed <- getWord16le
  leftSiblingAddress <- getMaybeAddress
  rightSiblingAddress <- getMaybeAddress
  case nodeType of
    BLinkTreeNodeEnumRawData ->
      case maybeDataspace of
        Nothing -> fail "cannot parse raw data node: got no data space"
        Just dataspaceDimensions' -> do
          let getChunk :: Get a -> Get (ChunkInfo a)
              getChunk getAddress' =
                ChunkInfo
                  <$> getWord32le
                  <*> getWord32le
                  <*> replicateM (length dataspaceDimensions') getLength
                  <*> (getLength *> getAddress')
          chunks <- replicateM (fromIntegral entriesUsed) (getChunk getAddress)
          lastChunk <- getChunk (pure Nothing)
          pure
            ( BLinkTreeNodeChunkedRawData
                nodeLevel
                entriesUsed
                leftSiblingAddress
                rightSiblingAddress
                chunks
                lastChunk
            )
    BLinkTreeNodeEnumGroup -> do
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
    3 -> pure LayoutClassVirtual
    n -> fail ("invalid data storage layout class value " <> show n)

getDataspaceType :: Get DataspaceType
getDataspaceType = do
  v <- getWord8
  case v of
    0 -> pure DataspaceScalar
    1 -> pure DataspaceSimple
    2 -> pure DataspaceNull
    _ -> fail ("dataspace type wasn't 0, 1 or 2 but " <> show v)

getDataspaceMessageData :: Get DataspaceMessageData
getDataspaceMessageData = do
  -- This value is used to determine the format of the Dataspace
  -- Message. When the format of the information in the message is
  -- changed, the version number is incremented and can be used to
  -- determine how the information in the object header is formatted.
  -- This document describes version one (1) (there was no version
  -- zero (0)).
  version <- getWord8
  when (version /= 1 && version /= 2) (fail ("dataspace version is not 1 or 2 but " <> show version))
  -- This value is the number of dimensions that the data object has.
  dimensionality <- getWord8
  -- This field is used to store flags to indicate the presence of
  -- parts of this message. Bit 0 (the least significant bit) is used
  -- to indicate that maximum dimensions are present. Bit 1 is used to
  -- indicate that permutation indices are present.
  flags <- getWord8
  type' <- if version == 1 then pure Nothing else Just <$> getDataspaceType
  let maxDimsStored :: Bool
      maxDimsStored = flags .&. 1 > 0
      permutationIndicesStored = flags .&. 2 > 0
  -- Reserved
  when (version == 1) (skipLabeled "reserved in version 1" 5)
  if dimensionality == 0
    then
      pure
        ( DataspaceMessageData
            { dataspaceDimensions = [],
              dataspaceType = type',
              dataspacePermutationIndices = []
            }
        )
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
      pure
        ( DataspaceMessageData
            { dataspaceDimensions = zipWith DataspaceDimension dimensions maxSizes,
              dataspaceType = type',
              dataspacePermutationIndices = permutationIndices
            }
        )

data CompoundDatatypeMemberV1 = CompoundDatatypeMemberV1
  { cdm1Name :: !BS.ByteString,
    cdm1ByteOffset :: !Word32,
    cdm1Dimensions :: ![Word32],
    cdm1Datatype :: !DatatypeMessageData
  }
  deriving (Show)

data CompoundDatatypeMemberV2 = CompoundDatatypeMemberV2
  { cdm2Name :: !BS.ByteString,
    cdm2ByteOffset :: !Word32,
    cdm2Datatype :: !DatatypeMessageData
  }
  deriving (Show)

getCompoundDatatypeMemberV1 :: Get CompoundDatatypeMemberV1
getCompoundDatatypeMemberV1 = do
  name <- BSL.toStrict <$> getLazyByteStringNul
  skipTo8 (BS.length (debugLog "compound member name v1" name) + 1)
  byteOffset <- getWord32le
  dimensionality <- getWord8
  skipLabeled "reserved" 3
  skipLabeled "dimension permutation" 4
  skipLabeled "reserved" 4
  -- dimensionality gives us the acutal count, but we have ot read four dimensions always
  dimensions <- replicateM 4 getWord32le
  CompoundDatatypeMemberV1
    name
    byteOffset
    (take (fromIntegral dimensionality) dimensions)
    <$> getDatatypeMessageData

getCompoundDatatypeMemberV2 :: Get CompoundDatatypeMemberV2
getCompoundDatatypeMemberV2 = do
  name <- BSL.toStrict <$> getLazyByteStringNul
  skipTo8 (BS.length (debugLog "compound member name" name) + 1)
  byteOffset <- getWord32le
  CompoundDatatypeMemberV2 name byteOffset <$> getDatatypeMessageData

skipLabeled :: (Integral a) => String -> a -> Get ()
skipLabeled _ = skip . fromIntegral

convertDatatypeIntoIntReader :: Datatype -> Get Int
convertDatatypeIntoIntReader (DatatypeFixedPoint {fixedPointDataElementSize = 1}) = label "datatype as int, 1 byte" (fromIntegral <$> getWord8)
convertDatatypeIntoIntReader (DatatypeFixedPoint {fixedPointDataElementSize = 2, fixedPointByteOrder = BigEndian}) = label "datatype as int, 2 byte BE" (fromIntegral <$> getWord16be)
convertDatatypeIntoIntReader (DatatypeFixedPoint {fixedPointDataElementSize = 4, fixedPointByteOrder = BigEndian}) = label "datatype as int, 4 byte BE" (fromIntegral <$> getWord32be)
convertDatatypeIntoIntReader (DatatypeFixedPoint {fixedPointDataElementSize = 4, fixedPointByteOrder = LittleEndian}) = label "datatype as int, 4 byte LE" (fromIntegral <$> getWord32le)
convertDatatypeIntoIntReader dt = fail $ "expected an integral datatype, but got the following: " <> show dt

getDatatypeMessageData :: Get DatatypeMessageData
getDatatypeMessageData = do
  classAndVersion <- getWord8
  let classNumeric = classAndVersion .&. 0b1111
      version = (classAndVersion .&. 0b11110000) `shiftR` 4
  when (version == 0) (fail "datatype message with version 0 invalid")
  class' <- label "datatype message, class" (getDatatypeClass classNumeric)
  bits0to7 <- getWord8
  bits8to15 <- getWord8
  _bits16to23 <- getWord8
  -- The size of a datatype element in bytes.
  size <- getWord32le
  case debugLog "class" class' of
    ClassEnumerated -> do
      -- We read two bytes, but enumeration wants one 2-byte little-endian value again, so we reconstitute
      let numberOfMembers = (bits8to15 `shiftL` 8) .|. bits0to7
      baseType <- label "enumeration, label type" getDatatypeMessageData
      let getAndPadEnumLabel = do
            bs <- label "enumeration label string" getLazyByteStringNul
            skipTo8 (BSL.length bs + 1)
            pure bs
      names <- replicateM' numberOfMembers (label "enumeration label string, padded" getAndPadEnumLabel)
      values <- replicateM' numberOfMembers (convertDatatypeIntoIntReader (datatypeClass baseType))
      pure (DatatypeMessageData version $ DatatypeEnumeration (datatypeClass baseType) $ zip names values)
    ClassFixedPoint -> do
      let byteOrder = if bits0to7 .&. 1 == 0 then LittleEndian else BigEndian
          loPadBit = bits0to7 .&. 0b0000010 > 0
          hiPadBit = bits0to7 .&. 0b0000100 > 0
          signed = bits0to7 .&. 0b0001000 > 0
      bitOffset <- getWord16le
      bitPrecision <- getWord16le
      pure
        ( DatatypeMessageData
            version
            ( DatatypeFixedPoint
                size
                byteOrder
                loPadBit
                hiPadBit
                signed
                bitOffset
                bitPrecision
            )
        )
    ClassVariableLength -> do
      let variableType = bits0to7 .&. 0b1111
      case variableType of
        0 -> pure (DatatypeMessageData version DatatypeVariableLengthSequence)
        1 -> do
          let paddingTypeNumeric = (bits0to7 .&. 0b11110000) `shiftR` 4
          paddingType <- if paddingTypeNumeric == 0 then pure PaddingNullTerminate else if paddingTypeNumeric == 1 then pure PaddingNull else if paddingTypeNumeric == 2 then pure PaddingSpace else fail ("invalid variable length string padding type " <> show paddingTypeNumeric)
          let characterSetNumeric = bits8to15 .&. 0b1111
          characterSet <- if characterSetNumeric == 0 then pure CharacterSetAscii else if characterSetNumeric == 1 then pure CharacterSetUtf8 else fail ("invalid variable length string character set " <> show characterSetNumeric)
          -- for now, skip the content (the size)
          -- commented in for now: we use getRemainingLazyByteString to ignore everything we don't read here
          -- skip (fromIntegral size)
          pure (DatatypeMessageData version (DatatypeVariableLengthString paddingType characterSet size))
        _ -> fail $ "variable length which is neither sequence nor string, bits are: " <> show variableType
    ClassString -> do
      let paddingTypeNumeric = bits0to7 .&. 0b1111
      paddingType <- if paddingTypeNumeric == 0 then pure PaddingNullTerminate else if paddingTypeNumeric == 1 then pure PaddingNull else if paddingTypeNumeric == 2 then pure PaddingSpace else fail ("invalid variable length string padding type " <> show paddingTypeNumeric)
      let characterSetNumeric = bits8to15 .&. 0b1111
      characterSet <- if characterSetNumeric == 0 then pure CharacterSetAscii else if characterSetNumeric == 1 then pure CharacterSetUtf8 else fail ("invalid variable length string character set " <> show characterSetNumeric)
      pure (DatatypeMessageData version (DatatypeString paddingType characterSet size))
    ClassFloatingPoint -> do
      when (bits0to7 .&. 1 == 3) (fail "floating point values with VAX-endianness are not supported")
      let byteOrder = if bits0to7 .&. 1 == 0 then LittleEndian else BigEndian
          mantissaNormalizationBits = bits0to7 .&. 0b11000
      mantissaNormalization <-
        case mantissaNormalizationBits of
          0 -> pure NoNormalization
          1 -> pure MostSignificantMantissaAlwaysSet
          2 -> pure MostSignificantMantissaImplied
          _ -> fail ("invalid mantissa normalization for floating point value " <> show mantissaNormalizationBits)

      bitOffset <- getWord16le
      bitPrecision <- getWord16le
      exponentLocation <- getWord8
      exponentSize <- getWord8
      mantissaLocation <- getWord8
      mantissaSize <- getWord8
      exponentBias <- getWord32le

      pure
        ( DatatypeMessageData
            version
            ( DatatypeFloatingPoint
                byteOrder
                (bits0to7 .&. 0b10 > 0)
                (bits0to7 .&. 0b100 > 0)
                (bits0to7 .&. 0b1000 > 0)
                mantissaNormalization
                (fromIntegral bits8to15)
                bitOffset
                bitPrecision
                exponentLocation
                exponentSize
                mantissaLocation
                mantissaSize
                exponentBias
            )
        )
    ClassCompound -> do
      let numberOfMembers = bits0to7 .|. (bits8to15 `shiftL` 8)
      case version of
        1 -> do
          members <- replicateM (fromIntegral (debugLog ("compound datatype member count: " <> show bits0to7 <> ", " <> show bits8to15) numberOfMembers)) getCompoundDatatypeMemberV1
          pure (DatatypeMessageData version (DatatypeCompoundV1 (debugLog "members" members)))
        2 -> do
          members <- replicateM (fromIntegral (debugLog ("compound datatype member count: " <> show bits0to7 <> ", " <> show bits8to15) numberOfMembers)) getCompoundDatatypeMemberV2
          pure (DatatypeMessageData version (DatatypeCompoundV2 (debugLog "members" members)))
        _ -> fail ("class Compound, version " <> show version <> " properties not supported yet")
    ClassArray ->
      if version == 1 || version == 2
        then do
          dimensionality <- getWord8
          skipLabeled "reserved" 3
          sizes <- replicateM (fromIntegral dimensionality) getWord32le
          replicateM_ (fromIntegral dimensionality) (skipLabeled "permutations not supported" 4)
          baseType <- getDatatypeMessageData
          -- fail ("base type: " <> show (BSL.unpack remainder))
          pure
            ( DatatypeMessageData
                version
                (DatatypeArray {arraySizes = sizes, arrayBaseType = datatypeClass baseType})
            )
        else fail ("class Array, version " <> show version <> " properties not supported yet")
    ClassReference -> do
      referenceType <-
        case bits0to7 of
          0 -> pure ObjectReference
          1 -> pure DatasetRegionReference
          n -> fail ("reference value " <> show n <> " not supported (can only do 0 or 1)")
      pure
        ( DatatypeMessageData
            version
            (DatatypeReference referenceType)
        )
    ClassOpaque -> do
      tag <- getByteString' bits0to7
      pure
        ( DatatypeMessageData
            version
            (DatatypeOpaqaue tag)
        )
    _ -> fail ("class " <> show class' <> ", version " <> show version <> " properties not supported yet")

skipTo8 :: (Integral a) => a -> Get ()
skipTo8 s = do
  let r = s `mod` 8
  when (r > 0) (skip (fromIntegral (8 - r)))

data LinkTypeEnum
  = HardLinkEnum
  | SoftLinkEnum
  | ExternalLinkEnum
  | UserDefinedLinkEnum !Word8
  | ReservedLinkEnum
  deriving (Show)

getLinkTypeEnum :: Get LinkTypeEnum
getLinkTypeEnum = do
  v <- getWord8
  case v of
    0 -> pure HardLinkEnum
    1 -> pure SoftLinkEnum
    64 -> pure ExternalLinkEnum
    reserved ->
      if reserved > 64
        then pure (UserDefinedLinkEnum reserved)
        else pure ReservedLinkEnum

data LinkType
  = HardLink !Address
  | SoftLink !BS.ByteString
  | ExternalLink {externalLinkFileName :: !BS.ByteString, externalLinkPath :: !BS.ByteString}
  | UserDefinedLink {userDefinedLinkCode :: !Word8, userDefinedLinkData :: !BS.ByteString}
  | ReservedLink
  deriving (Show)

getLinkType :: LinkTypeEnum -> Get LinkType
getLinkType HardLinkEnum = HardLink <$> getAddress
getLinkType SoftLinkEnum = do
  lengthOfLink <- getWord16le
  SoftLink <$> getByteString' lengthOfLink
getLinkType ExternalLinkEnum = do
  _lengthOfLink <- getWord16le
  versionNumberAndFlags <- getWord8
  when (versionNumberAndFlags /= 0) (fail "invalid external link: the first byte should be the version number and flags, which are supposed to be zero, but they are not")
  (ExternalLink . BSL.toStrict <$> getLazyByteStringNul) <*> (BSL.toStrict <$> getLazyByteStringNul)
getLinkType (UserDefinedLinkEnum linkCode) = do
  lengthOfData <- getWord16le
  UserDefinedLink linkCode <$> getByteString' lengthOfData
getLinkType ReservedLinkEnum = pure ReservedLink

getAttributeContentByType :: Datatype -> Get AttributeContent
getAttributeContentByType (DatatypeString _padding _charset _size) =
  AttributeContentFixedString . BSL.toStrict <$> getRemainingLazyByteString
getAttributeContentByType (DatatypeFixedPoint {fixedPointDataElementSize = 1}) = do
  result <- AttributeContentIntegral . fromIntegral <$> getWord8
  pure result
getAttributeContentByType (DatatypeFixedPoint {fixedPointDataElementSize = 2, fixedPointByteOrder = BigEndian}) = do
  result <- AttributeContentIntegral . fromIntegral <$> getWord16be
  pure result
getAttributeContentByType (DatatypeFixedPoint {fixedPointDataElementSize = 4, fixedPointByteOrder = BigEndian}) = do
  result <- AttributeContentIntegral . fromIntegral <$> getWord32be
  pure result
getAttributeContentByType (DatatypeFixedPoint {fixedPointDataElementSize = 4, fixedPointByteOrder = LittleEndian}) = do
  result <- AttributeContentIntegral . fromIntegral <$> getWord32le
  pure result
getAttributeContentByType (DatatypeFixedPoint {fixedPointDataElementSize = 8, fixedPointByteOrder = LittleEndian}) = do
  result <- AttributeContentIntegral . fromIntegral <$> getWord64le
  pure result
getAttributeContentByType (DatatypeFixedPoint {fixedPointDataElementSize = 8, fixedPointByteOrder = BigEndian}) = do
  result <- AttributeContentIntegral . fromIntegral <$> getWord64be
  pure result
getAttributeContentByType (DatatypeFloatingPoint {floatingPointBitPrecision = 32, floatingPointByteOrder = BigEndian}) = do
  result <- AttributeContentFloating . realToFrac <$> getFloat32be
  pure result
getAttributeContentByType (DatatypeFloatingPoint {floatingPointBitPrecision = 64, floatingPointByteOrder = LittleEndian}) = do
  result <- AttributeContentFloating . realToFrac <$> getFloat64le
  pure result
getAttributeContentByType (DatatypeEnumeration baseType enumMap) = AttributeContentEnumeration enumMap <$> convertDatatypeIntoIntReader baseType
getAttributeContentByType (DatatypeVariableLengthString PaddingNullTerminate charset _size) = do
  size' <- getWord32le
  globalHeapAddress <- getWord64le
  objectIndex <- getWord32le
  pure
    ( AttributeContentVariableString
        globalHeapAddress
        objectIndex
        size'
        charset
    )
getAttributeContentByType (DatatypeReference referenceType) = do
  content <- getRemainingLazyByteString
  pure
    ( AttributeContentReference referenceType content
    )
getAttributeContentByType (DatatypeCompoundV1 members) = do
  let convertMember (CompoundDatatypeMemberV1 {cdm1Datatype = DatatypeMessageData {datatypeClass}, cdm1Name}) = do
        convertedType <- label ("compound member " <> show cdm1Name) (getAttributeContentByType datatypeClass)
        pure (AttributeContentCompoundMember cdm1Name convertedType)
  membersConverted <- mapM convertMember members
  pure (AttributeContentCompound membersConverted)
getAttributeContentByType type' = do
  remainder <- getRemainingLazyByteString
  pure
    $ trace
      ("attributo type: " <> show type')
    $ AttributeContentTodo type' remainder

getMessage :: Word16 -> Get Message
getMessage 0x0000 = do
  void getRemainingLazyByteString
  pure NilMessage
-- The dataspace message describes the number of dimensions (in other
-- words, “rank”) and size of each dimension that the data object has.
-- This message is only used for datasets which have a simple,
-- rectilinear, array-like layout; datasets requiring a more complex
-- layout are not yet supported.
getMessage 0x0001 = DataspaceMessage <$> getDataspaceMessageData
getMessage 0x0002 = do
  version <- getWord8
  when (version /= 0) (fail ("invalid link info message version (is not 0) " <> show version))
  flags <- getWord8
  maximumCreationIndex <-
    if flags .&. 1 > 0
      then Just <$> getWord64le
      else pure Nothing
  fractalHeapAddress <- getMaybeAddress
  btreeAddress <- getMaybeAddress
  btreeCreationOrderIndex <-
    if flags .&. 2 > 0
      then Just <$> getWord64le
      else pure Nothing
  void getRemainingLazyByteString
  pure
    ( LinkInfoMessage
        ( LinkInfoMessageData
            { linkInfoMaximumCreationIndex = maximumCreationIndex,
              linkInfoFractalHeapAddress = fractalHeapAddress,
              linkInfoBtreeAddress = btreeAddress,
              linkInfoBtreeCreationOrderIndex = btreeCreationOrderIndex
            }
        )
    )
-- Explanation of "datatype" in general
--
-- https://support.hdfgroup.org/documentation/hdf5/latest/_l_b_datatypes.html
getMessage 0x0003 = do
  m <- DatatypeMessage <$> getDatatypeMessageData
  void getRemainingLazyByteString
  pure m
-- IV.A.2.e. The Data Storage - Fill Value (Old) Message
getMessage 0x0004 = do
  size <- label "data storage fill value old, size" getWord32le
  _fillValue <- label "data storage fill value old, fill value" (getByteString' size)
  pure DataStorageFillValueOldMessage
getMessage 0x0005 = do
  version <- getWord8
  case version of
    1 -> do
      _spaceAllocationTime <- getDataStorageSpaceAllocationTime
      _fillValueWriteTime <- getDataStorageFillValueWriteTime
      _fillValueDefined <- getWord8
      size <- getWord32le
      -- TODO: the actual fill value depends on the datatype for the
      -- dataset - this must be a parameter to this function then
      _fillValue <- getByteString (fromIntegral size)
      pure DataStorageFillValueMessage
    2 -> do
      _spaceAllocationTime <- getDataStorageSpaceAllocationTime
      _fillValueWriteTime <- getDataStorageFillValueWriteTime
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
getMessage 0x0006 = do
  version <- getWord8
  when (version /= 1) (fail ("invalid link info message version (is not 1) " <> show version))
  flags <- getWord8
  let firstTwoBits = flags .&. 0b11
      getLengthOfLinkName :: Get Word64
      getLengthOfLinkName =
        case firstTwoBits of
          0 -> fromIntegral <$> getWord8
          1 -> fromIntegral <$> getWord16le
          2 -> fromIntegral <$> getWord32le
          _ -> getWord64le
  linkTypeEnum <-
    if flags .&. 0b1000 > 0
      then getLinkTypeEnum
      else pure HardLinkEnum
  creationOrder <-
    if flags .&. 0b100 > 0
      then Just <$> getWord64le
      else pure Nothing
  linkNameCharacterSet <-
    if flags .&. 0b10000 > 0
      then Just <$> getWord8
      else pure Nothing
  lengthOfLinkName <- getLengthOfLinkName
  linkName <- getByteString' lengthOfLinkName
  linkType <- getLinkType linkTypeEnum
  void getRemainingLazyByteString
  pure (LinkMessage (LinkMessageData creationOrder linkNameCharacterSet linkName linkType))
getMessage 0x000a = do
  version <- getWord8
  when (version /= 0) (fail ("group info message has invalid version " <> show version))
  flags <- getWord8
  maxCompactValueLinkPhaseChange <-
    if flags .&. 1 > 0
      then Just <$> getWord16le
      else pure Nothing
  maxDenseValueLinkPhaseChange <-
    if flags .&. 1 > 0
      then Just <$> getWord16le
      else pure Nothing
  estimatedNumberOfEntries <-
    if flags .&. 2 > 0
      then Just <$> getWord16le
      else pure Nothing
  estimatedLinkNameLengthOfEntries <-
    if flags .&. 2 > 0
      then Just <$> getWord16le
      else pure Nothing
  void getRemainingLazyByteString
  pure
    ( GroupInfoMessage
        ( GroupInfoMessageData
            maxCompactValueLinkPhaseChange
            maxDenseValueLinkPhaseChange
            estimatedNumberOfEntries
            estimatedLinkNameLengthOfEntries
        )
    )
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
getMessage 0x000e = do
  let readNumber noBytes = do
        n <- getByteString noBytes
        case decimal (decodeLatin1 n) of
          Left _ -> fail ("expected a number in ASCII, got these bytes: " <> show (BS.unpack n))
          Right (v, _) -> pure v
  ObjectModificationTimeOldMessage
    <$> readNumber 4
    <*> readNumber 2
    <*> readNumber 2
    <*> readNumber 2
    <*> readNumber 2
    <*> (readNumber 2 <* skipLabeled "reserved" 2)
getMessage 0x0008 = do
  version <- getWord8
  let getCompactLayout = do
        -- (Note: The dimensionality information is in the Dataspace message)
        -- This field contains the size of the raw data for the
        -- dataset array, in bytes.
        size <- getWord16le

        -- This field contains the raw data for the dataset array.
        -- TODO: interpretation of this data needs type information
        void $ getByteString (fromIntegral size)

        pure (LayoutCompact size)
      getContiguousLayout = do
        -- This is the address of the raw data in the file. The
        -- address may have the undefined address value, to indicate
        -- that storage has not yet been allocated for this array.
        rawDataAddress <- getMaybeAddress
        size <- getLength
        -- Apparently theres "crap" at the end of this message?
        void getRemainingLazyByteString
        pure (LayoutContiguous rawDataAddress size)
  case version of
    4 -> do
      layoutClass <- label "data layout, version 4 layout class" getDataStorageLayoutClass
      case layoutClass of
        -- Order of appearance in the spec
        LayoutClassCompact -> DataStorageLayoutMessage <$> getCompactLayout
        LayoutClassContiguous -> DataStorageLayoutMessage <$> getContiguousLayout
        LayoutClassVirtual -> DataStorageLayoutMessage <$> (LayoutVirtualStorage <$> getAddress <*> getWord32le)
        -- Chunked version 3 is different from chunked version 4
        LayoutClassChunked -> do
          flagsRaw <- getWord8
          dimensionality <- getWord8
          dimensionSize <- getWord8
          let flags =
                ([DoNotApplyFilterToPartialEdgeChunk | flagsRaw .&. 1 > 0])
                  <> ([SingleIndexWithFilter | flagsRaw .&. 2 > 0])
              readDimension :: Get Word32
              readDimension = case dimensionSize of
                1 -> fromIntegral <$> getWord8
                2 -> fromIntegral <$> getWord16le
                4 -> getWord32le
                n -> fail ("invalid v4 chunk dimension size " <> show n)
          dimensionSizes <- replicateM' dimensionality readDimension

          indexingType <- getWord8

          indexingInformation <- case indexingType of
            1 -> LayoutChunkedV4IndexingSingleChunk <$> getWord32le <*> getWord32le
            2 -> pure LayoutChunkedV4IndexingImplicit
            3 -> LayoutChunkedV4IndexingFixedArray <$> getWord8
            4 -> LayoutChunkedV4IndexingExtensibleArray <$> getWord8 <*> getWord8 <*> getWord8 <*> getWord8 <*> getWord8
            5 -> LayoutChunkedV4IndexingV2BTree <$> getWord32le <*> getWord8 <*> getWord8
            n -> fail ("invalid chunk v4 indexing information type " <> show n)

          pure (DataStorageLayoutMessage (LayoutChunkedV4 dimensionSizes flags indexingInformation))
    3 -> do
      layoutClass <- getDataStorageLayoutClass

      case layoutClass of
        -- Order of appearance in the spec
        LayoutClassCompact -> DataStorageLayoutMessage <$> getCompactLayout
        LayoutClassContiguous -> DataStorageLayoutMessage <$> getContiguousLayout
        -- Chunked version 3 is different from chunked version 4
        LayoutClassChunked -> do
          -- A chunk has a fixed dimensionality. This field specifies
          -- the number of dimension size fields later in the message.
          dimensionality <- getWord8
          -- skip 3
          -- This is the address of the III.A.1. Disk Format: Level
          -- 1A1 - Version 1 B-trees that is used to look up the
          -- addresses of the chunks that actually store portions of
          -- the array data.
          address <- getAddress
          -- These values define the dimension size of a single chunk,
          -- in units of array elements (not bytes). The first
          -- dimension stored in the list of dimensions is the slowest
          -- changing dimension and the last dimension stored is the
          -- fastest changing dimension.
          --
          -- The spec isn't explicit about it, but it seems like the dimensionality has to be "minus-oned"
          sizes <- replicateM (fromIntegral dimensionality - 1) getWord32le
          datasetElementSize <- getWord32le
          -- The need to skip this is _very_ weird. Not sure why we need it, it's not in the spec.
          -- skip 1
          void getRemainingLazyByteString

          pure (DataStorageLayoutMessage (LayoutChunked address sizes datasetElementSize))
        n -> fail ("invalid v3 storage class layout " <> show n)
    _ -> do
      when (version /= 1 && version /= 2) (fail ("version of the data layout message is not supported yet: " <> show version))
      dimensionality <- getWord8
      layoutClass <- getDataStorageLayoutClass
      -- Reserved
      skip 5
      case layoutClass of
        LayoutClassContiguous -> do
          rawDataAddress <- getAddress
          sizes <- replicateM (fromIntegral dimensionality) getWord32le
          -- For some unknown reason we're getting a few bytes more sometimes
          void getRemainingLazyByteString
          pure (DataStorageLayoutMessage (LayoutContiguousOld rawDataAddress sizes))
        LayoutClassChunked -> do
          bTreeAddress <- getAddress
          sizes <- replicateM (fromIntegral dimensionality) getWord32le
          DataStorageLayoutMessage . LayoutChunked bTreeAddress sizes <$> getWord32le
        LayoutClassCompact -> do
          sizes <- replicateM (fromIntegral dimensionality) getWord32le
          compactDataSize <- getWord32le
          -- TODO: We need the datatype size to read the actual compact data
          pure (DataStorageLayoutMessage (LayoutCompactOld sizes compactDataSize))
getMessage 0x0010 = ObjectHeaderContinuationMessage <$> getAddress <*> getLength
getMessage 0x0011 = SymbolTableMessage <$> (SymbolTableMessageData <$> getAddress <*> getAddress)
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
  name <- getByteString (fromIntegral (debugLog "name size" nameSize))
  skipTo8 (debugLog ("skipping after name " <> show (BS.unpack name)) nameSize)
  datatypeMessage <- isolate (fromIntegral datatypeSize) (getDatatypeMessageData <* getRemainingLazyByteString)
  skipTo8 (trace ("datatype: " <> show datatypeMessage) datatypeSize)
  dataspaceMessage <- getDataspaceMessageData
  skipTo8 (debugLog "dataspace size" dataspaceSize)
  attributeContent' <- label ("attribute " <> show (datatypeClass datatypeMessage) <> " message type") (getAttributeContentByType (datatypeClass datatypeMessage))
  void getRemainingLazyByteString
  pure
    ( debugLog "attribute message" $
        AttributeMessage
          ( AttributeData
              name
              datatypeMessage
              dataspaceMessage
              attributeContent'
          )
    )
-- IV.A.2.v. The Attribute Info Message
getMessage 0x0015 = do
  version <- getWord8
  when (version /= 0) (fail ("invalid attribute info message version " <> show version))
  flags <- getWord8
  maximumCreationIndex <-
    if flags .&. 0b1 > 0
      then Just <$> getWord16le
      else pure Nothing
  fractalHeapAddress <- getAddress
  attributeNamev2BtreeAddress <- getAddress
  attributeCreationOrderv2BtreeAddress <-
    if flags .&. 0b10 > 0
      then Just <$> getAddress
      else pure Nothing
  pure
    ( AttributeInfoMessage $
        AttributeInfoMessageData
          { attributeInfoMaximumCreationIndex = maximumCreationIndex,
            attributeInfoFractalHeapAddress = fractalHeapAddress,
            attributeInfoAttributeNamev2BtreeAddress = attributeNamev2BtreeAddress,
            attributeInfoCreationOrderv2BtreeAddress = attributeCreationOrderv2BtreeAddress
          }
    )
getMessage n = fail ("invalid message type " <> show n)

getGlobalHeapObject :: Get GlobalHeapObject
getGlobalHeapObject = do
  objectIndex <- getWord16le
  objectReferenceCount <- getWord16le
  -- Reserved
  skip 4
  size' <- getLength
  data' <- getByteString (fromIntegral size')
  pure (GlobalHeapObject objectIndex objectReferenceCount data')

getGlobalHeap :: Get GlobalHeap
getGlobalHeap = do
  signature' <- getByteString 4
  -- "GCOL" in ASCII
  when (signature' /= BS.pack [0x47, 0x43, 0x4f, 0x4c]) (fail "invalid global heap signature")
  version <- getWord8
  -- Reserved
  skipLabeled "reserved" 3
  collectionSize <- getLength
  let getNextGlobalHeapObject :: Get [GlobalHeapObject]
      getNextGlobalHeapObject = do
        bytesSoFar <- bytesRead
        -- No bytes left
        if bytesSoFar >= fromIntegral collectionSize - 16
          then pure []
          else do
            objectIndex <- label "heap object index" getWord16le
            if objectIndex == 0
              then pure []
              else do
                objectReferenceCount <- label "heap object reference count" getWord16le
                -- Reserved
                skipLabeled "reserved" 4
                size' <- label "heap object length" getLength
                data' <- label "heap object content" (getByteString (fromIntegral size'))

                let toSkip = 8 - (size' `mod` 8)
                when (toSkip /= 8) (skipLabeled "padding after heap content" toSkip)

                nextObjects <- getNextGlobalHeapObject
                let thisObject =
                      GlobalHeapObject
                        objectIndex
                        objectReferenceCount
                        data'
                pure (thisObject : nextObjects)

  objects <- getNextGlobalHeapObject
  pure (GlobalHeap version objects)

getUnixSecondsSinceEpoch :: Get UTCTime
getUnixSecondsSinceEpoch = posixSecondsToUTCTime . secondsToNominalDiffTime . fromIntegral <$> getWord32le

getObjectHeaderTimes :: Get ObjectHeaderTimes
getObjectHeaderTimes =
  ObjectHeaderTimes
    <$> getUnixSecondsSinceEpoch
    <*> getUnixSecondsSinceEpoch
    <*> getUnixSecondsSinceEpoch
    <*> getUnixSecondsSinceEpoch

getObjectHeaderV2 :: Get ObjectHeader
getObjectHeaderV2 = do
  signature' <- getByteString 4
  -- "OHDR" in ASCII
  when (signature' /= BS.pack [79, 72, 68, 82]) (fail ("invalid object header v2 signature: " <> show signature'))
  version <- getWord8
  when (version /= 2) (fail ("object header version was not 2 but " <> show version))
  objectHeaderFlags <- getWord8
  times <-
    if objectHeaderFlags .&. 0b100000 > 0
      then Just <$> label "object header v2 times" getObjectHeaderTimes
      else pure Nothing
  _maxCompact <- if objectHeaderFlags .&. 0b10000 > 0 then Just <$> getWord16le else pure Nothing
  _maxDense <- if objectHeaderFlags .&. 0b10000 > 0 then Just <$> getWord16le else pure Nothing
  let getSizeOfChunk0 =
        case objectHeaderFlags .&. 0b11 of
          0 -> fromIntegral <$> getWord8
          1 -> fromIntegral <$> getWord16le
          2 -> fromIntegral <$> getWord32le
          3 -> getWord64le
          n -> fail ("size of chunk #0 is not 0-3 but " <> show n)
  sizeOfChunk0 <- getSizeOfChunk0
  let readMessage = do
        messageType <- getWord8
        headerMessageDataSize <- getWord16le
        flags <- getWord8
        when (objectHeaderFlags .&. 0b100 > 0) (void getWord16le)
        if (debugLog ("v2 message type " <> show messageType <> ", flags " <> show flags) (flags .&. 0b10)) > 0
          then do
            sharedVersion <- getWord8
            sharedMessageType' <- getWord8
            case sharedVersion of
              2 -> do
                sharedMessageType'' <- case sharedMessageType' of
                  0 -> fail ("flags " <> show flags <> " indicate a shared message, but the type is 0, which is not shared at all")
                  1 -> SharedMessageTypeShared <$> getWord64le
                  2 -> SharedMessageTypeCommitted <$> getAddress
                  3 -> pure SharedMessageShareable
                  smv -> fail ("invalid shared message type " <> show smv)
                skipLabeled "zeroes after body" (headerMessageDataSize - 10)
                pure
                  ( SharedMessage
                      ( SharedMessageData
                          (fromIntegral messageType)
                          sharedMessageType''
                      )
                  )
              v -> fail ("invalid version of shared message: " <> show v <> ", message flags were " <> show flags)
          else
            isolate
              (fromIntegral (debugLog ("v2 non-shared message type " <> show messageType <> ", flags " <> show flags) headerMessageDataSize))
              ( label
                  "message inside v2 object header"
                  (getMessage (fromIntegral messageType))
                  <* getRemainingLazyByteString
              )
      decodeMessages = do
        bytesRead' <- bytesRead
        if sizeOfChunk0 - fromIntegral bytesRead' <= 9
          then pure []
          else do
            m <- readMessage
            ms <- decodeMessages
            pure (m : ms)
  messages <-
    isolate
      (fromIntegral sizeOfChunk0)
      decodeMessages
  pure
    ( ObjectHeader
        { ohVersion = version,
          ohObjectReferenceCount = 1,
          ohTimes = times,
          ohHeaderMessages = messages
        }
    )

getObjectHeaderV1 :: Get ObjectHeader
getObjectHeaderV1 = do
  version <- getWord8
  when (version /= 1) (fail ("object header version was not 1 but " <> show version))
  skipLabeled "reserved" 1
  messageCount <- getWord16le
  objectReferenceCount <- getWord32le
  objectHeaderSize <- getWord32le
  -- Not sure why we skipped this? Padding?
  skipLabeled "unknown" 4
  -- The following code looks a little weird, but the problem is that
  -- "messageCount" above doesn't give us the number of messages
  -- _only_ in this object header. It could give us the number "4",
  -- but there is just one message here: a continuation message. So we
  -- isolate to the number of bytes we expect and read until we don't
  -- have any bytes anymore.
  let readMessage = do
        messageType <- getWord16le
        headerMessageDataSize <- getWord16le
        flags <- getWord8
        skipLabeled "reserved" 3
        if flags .&. 0b10 > 0
          then do
            sharedVersion <- getWord8
            sharedMessageType' <- getWord8
            case sharedVersion of
              2 -> do
                sharedMessageType'' <- case sharedMessageType' of
                  0 -> fail ("flags " <> show flags <> " indicate a shared message, but the type is 0, which is not shared at all")
                  1 -> SharedMessageTypeShared <$> getWord64le
                  2 -> SharedMessageTypeCommitted <$> getAddress
                  3 -> pure SharedMessageShareable
                  smv -> fail ("invalid shared message type " <> show smv)
                skipLabeled "zeroes after body" (headerMessageDataSize - 10)
                pure (SharedMessage (SharedMessageData messageType sharedMessageType''))
              v -> fail ("invalid version of shared message: " <> show v)
          else isolate (fromIntegral (debugLog "data size" headerMessageDataSize)) (label "message inside object header" (getMessage (debugLog "message type" messageType)) <* getRemainingLazyByteString)
      decodeMessages 0 = getRemainingLazyByteString >> pure []
      decodeMessages maxMessages = do
        bytesRead' <- bytesRead
        let remainder = bytesRead' `mod` 8
        skip (fromIntegral remainder)
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
  pure
    ( ObjectHeader
        { ohVersion = version,
          ohObjectReferenceCount = objectReferenceCount,
          ohHeaderMessages = messages,
          ohTimes = Nothing
        }
    )

getObjectHeader :: Get ObjectHeader
getObjectHeader = getObjectHeaderV1 <|> getObjectHeaderV2

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
              <*> ((fromJust <$>) <$> getSymbolTableEntry)
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
