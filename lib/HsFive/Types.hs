{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module HsFive.Types where

import Control.Monad (MonadPlus)
import Data.Binary (getWord8)
import Data.Binary.Get (bytesRead, getWord16le, isEmpty, isolate, runGet, runGetOrFail, skip)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Foldable (msum)
import qualified Data.List.NonEmpty as NE
import Data.Maybe (mapMaybe)
import Data.Text (Text, intercalate, unpack)
import Data.Text.Encoding (decodeUtf8Lenient)
import HsFive.CoreTypes
  ( Address,
    AttributeContent (AttributeContentString),
    BLinkTreeNode (BLinkTreeNodeChunkedRawData, BLinkTreeNodeGroup),
    DataStorageFilterPipelineMessageData (DataStorageFilterPipelineMessageData, dataStorageFilterPipelineFilters),
    DataStorageLayout,
    DataStoragePipelineFilter,
    DataspaceDimension,
    DataspaceMessageData (DataspaceMessageData, dataspaceDimensions, dataspacePermutationIndices),
    Datatype,
    DatatypeMessageData (DatatypeMessageData, datatypeClass),
    GroupSymbolTableNode,
    HeapWithData (HeapWithData),
    Length,
    Message (AttributeMessage, DataStorageFilterPipelineMessage, DataStorageLayoutMessage, DataspaceMessage, DatatypeMessage, ObjectHeaderContinuationMessage, SymbolTableMessage),
    Superblock,
    SymbolTableEntry,
    SymbolTableMessageData (SymbolTableMessageData, symbolTableMessageLocalHeapAddress, symbolTableMessageV1BTreeAddress),
    bltnChildPointers,
    bltnKeyOffsets,
    getBLinkTreeNode,
    getGroupSymbolTableNode,
    getHeapHeader,
    getMessage,
    getObjectHeaderV1,
    getSuperblock,
    gstnEntries,
    h5sbSymbolTableEntry,
    h5steLinkNameOffset,
    h5steObjectHeaderAddress,
    heapDataSegmentAddress,
    heapDataSegmentSize,
    ohHeaderMessages,
    readKey,
  )
import qualified HsFive.CoreTypes as CoreTypes
import Safe (headMay)
import System.File.OsPath (withBinaryFile)
import System.IO (Handle, IOMode (ReadMode), SeekMode (AbsoluteSeek, SeekFromEnd), hSeek, hTell)
import System.OsPath (OsPath)

newtype Path = Path (NE.NonEmpty Text)

instance Show Path where
  show (Path components) = unpack (intercalate "/" (NE.toList components))

lastComponent :: Path -> Text
lastComponent (Path components) = NE.last components

data Attribute = Attribute
  { attributeName :: !Text,
    attributeType :: !Datatype,
    attributeDimensions :: ![DataspaceDimension],
    attributePermutationIndices :: ![Length],
    attributeDataString :: !Text
  }
  deriving (Show)

data GroupData = GroupData
  { groupPath :: !Path,
    groupAttributes :: ![Attribute],
    groupChildren :: ![Node]
  }
  deriving (Show)

data DatasetData = DatasetData
  { datasetPath :: !Path,
    datasetDimensions :: ![DataspaceDimension],
    datasetPermutationIndices :: ![Length],
    datasetDatatype :: !Datatype,
    datasetFilters :: ![DataStoragePipelineFilter],
    datasetStorageLayout :: !DataStorageLayout,
    datasetAttributes :: ![Attribute]
  }
  deriving (Show)

data Node
  = GroupNode !GroupData
  | DatasetNode !DatasetData
  deriving (Show)

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

tryPick :: (Foldable t, MonadPlus m, Functor t) => (a1 -> m a2) -> t a1 -> m a2
tryPick f as = msum (f <$> as)

resolveContinuationMessages :: Handle -> [Message] -> IO [Message]
resolveContinuationMessages handle = foldMap (resolveContinuationMessage handle)

resolveContinuationMessage :: Handle -> Message -> IO [Message]
resolveContinuationMessage handle (ObjectHeaderContinuationMessage continuationAddress length') = do
  hSeek handle AbsoluteSeek (fromIntegral continuationAddress)
  data' <- BSL.hGet handle (fromIntegral length')
  -- FIXME: This is a duplicate from CoreTypes (almost), put all Binary stuff in there.
  let readMessage = do
        messageType <- getWord16le
        headerMessageDataSize <- getWord16le
        flags <- getWord8
        skip 3
        isolate (fromIntegral headerMessageDataSize) (getMessage messageType)
      decodeMessages = do
        bytesRead' <- bytesRead
        let remainder = bytesRead' `mod` 8
        skip (fromIntegral remainder)
        -- skip (fromIntegral remainder)
        empty <- isEmpty
        if empty
          then pure []
          else do
            m <- readMessage
            ms <- decodeMessages
            pure (m : ms)
  case runGetOrFail decodeMessages data' of
    Left (_, _, e') -> error ("parsing continuation messages failed: " <> show e')
    Right (_, _, messages) -> pure messages
resolveContinuationMessage _handle m = pure [m]

appendPath :: Path -> Text -> Path
appendPath (Path xs) x = Path (xs <> NE.singleton x)

singletonPath :: Text -> Path
singletonPath = Path . NE.singleton

readNode :: Handle -> Maybe Path -> Maybe HeapWithData -> SymbolTableEntry -> IO Node
readNode handle previousPath maybeHeap e = do
  hSeek handle AbsoluteSeek (fromIntegral (h5steObjectHeaderAddress e))
  objectHeaderData <- BSL.hGet handle 4096
  case runGetOrFail getObjectHeaderV1 objectHeaderData of
    Left (_, bytesConsumed, e') ->
      error
        ( "invalid object header at symbol table entry (consumed "
            <> show bytesConsumed
            <> " bytes): "
            <> show e'
        )
    Right (_, _, header) -> do
      let newPath :: Path
          newPath =
            case maybeHeap of
              Nothing -> singletonPath "/"
              Just (HeapWithData _heapHeader heapData'') ->
                case previousPath of
                  Nothing -> singletonPath "/"
                  Just previousPath' ->
                    appendPath
                      previousPath'
                      ( decodeUtf8Lenient $
                          BSL.toStrict $
                            BSL.takeWhile (/= 0) $
                              BSL.drop (fromIntegral (h5steLinkNameOffset e)) heapData''
                      )
      allMessages <- resolveContinuationMessages handle (ohHeaderMessages header)
      let filterAttribute (AttributeMessage attributeData) = Just attributeData
          filterAttribute _ = Nothing
          convertAttribute :: CoreTypes.AttributeData -> Attribute
          convertAttribute
            ( CoreTypes.AttributeData
                { CoreTypes.attributeName = an,
                  CoreTypes.attributeDatatypeMessageData = DatatypeMessageData {datatypeClass = typeData},
                  CoreTypes.attributeDataspaceMessageData = DataspaceMessageData {dataspaceDimensions, dataspacePermutationIndices},
                  CoreTypes.attributeContent = AttributeContentString content
                }
              ) =
              Attribute
                { attributeName = decodeUtf8Lenient $ BS.takeWhile (/= 0) an,
                  attributeType = typeData,
                  attributeDimensions = dataspaceDimensions,
                  attributePermutationIndices = dataspacePermutationIndices,
                  attributeDataString = decodeUtf8Lenient $ BS.takeWhile (/= 0) content
                }
          convertAttribute a = error $ "invalid attribute data, not a string: " <> show a
          attributes = convertAttribute <$> mapMaybe filterAttribute allMessages
          filterSymbolTable (SymbolTableMessage d) = Just d
          filterSymbolTable _ = Nothing
      case headMay (mapMaybe filterSymbolTable allMessages) of
        Nothing -> do
          let filterDataspace (DataspaceMessage d) = Just d
              filterDataspace _ = Nothing
              filterLayout (DataStorageLayoutMessage d) = Just d
              filterLayout _ = Nothing
              filterDatatype (DatatypeMessage d) = Just d
              filterDatatype _ = Nothing
              filterFilters (DataStorageFilterPipelineMessage d) = Just d
              filterFilters _ = Nothing
              searchMessage :: (Message -> Maybe a) -> Maybe a
              searchMessage finder = headMay (mapMaybe finder allMessages)
          case searchMessage filterDataspace of
            Nothing -> fail "dataset without dataspace"
            Just (DataspaceMessageData {dataspaceDimensions, dataspacePermutationIndices}) ->
              case searchMessage filterDatatype of
                Nothing -> fail "dataset without datatype"
                Just (DatatypeMessageData {datatypeClass}) ->
                  let filters = case searchMessage filterFilters of
                        Nothing -> []
                        Just (DataStorageFilterPipelineMessageData {dataStorageFilterPipelineFilters}) -> dataStorageFilterPipelineFilters
                   in case searchMessage filterLayout of
                        Nothing -> fail "dataset without layout"
                        Just layout ->
                          pure
                            ( DatasetNode
                                ( DatasetData
                                    { datasetDimensions = dataspaceDimensions,
                                      datasetPermutationIndices = dataspacePermutationIndices,
                                      datasetDatatype = datatypeClass,
                                      datasetFilters = filters,
                                      datasetStorageLayout = layout,
                                      datasetPath = newPath,
                                      datasetAttributes = attributes
                                    }
                                )
                            )
        Just (SymbolTableMessageData {symbolTableMessageV1BTreeAddress, symbolTableMessageLocalHeapAddress}) -> do
          hSeek handle AbsoluteSeek (fromIntegral symbolTableMessageV1BTreeAddress)
          blinkTreenode <- BSL.hGet handle 2048
          case runGetOrFail (getBLinkTreeNode Nothing Nothing) blinkTreenode of
            Left _ -> error "error reading b-link node"
            Right (_, _, BLinkTreeNodeChunkedRawData {}) -> fail "got chunked data node inside tree"
            Right (_, _, node@(BLinkTreeNodeGroup {})) -> do
              hSeek handle AbsoluteSeek (fromIntegral symbolTableMessageLocalHeapAddress)
              heapHeaderData <- BSL.hGet handle 2048
              case runGetOrFail getHeapHeader heapHeaderData of
                Left _ -> error "error reading heap"
                Right (_, _, heapHeader') -> do
                  hSeek handle AbsoluteSeek (fromIntegral (heapDataSegmentAddress heapHeader'))
                  heapData' <- BSL.hGet handle (fromIntegral (heapDataSegmentSize heapHeader'))
                  let keyAddressesOnHeap :: [Address]
                      keyAddressesOnHeap = (\len -> heapDataSegmentAddress heapHeader' + len) <$> bltnKeyOffsets node
                      childAddressesOnHeap = bltnChildPointers node
                      readChild :: Address -> IO GroupSymbolTableNode
                      readChild addr = do
                        hSeek handle AbsoluteSeek (fromIntegral addr)
                        rawData <- BSL.hGet handle 2048
                        pure (runGet getGroupSymbolTableNode rawData)
                  keysOnHeap <- mapM (readKey handle) keyAddressesOnHeap
                  childrenOnHeap <- mapM readChild childAddressesOnHeap

                  childNodes <-
                    mapM
                      (readNode handle (Just newPath) (Just (HeapWithData heapHeader' heapData')))
                      (concatMap gstnEntries childrenOnHeap)

                  pure
                    ( GroupNode
                        ( GroupData
                            { groupPath = newPath,
                              groupAttributes = attributes,
                              groupChildren = childNodes
                            }
                        )
                    )

readH5 :: OsPath -> IO GroupData
readH5 fileNameEncoded = do
  withBinaryFile fileNameEncoded ReadMode $ \handle -> do
    superblock <- readSuperblock handle

    case superblock of
      Nothing -> error "couldn't read superblock"
      Just superblock' -> do
        node <- readNode handle Nothing Nothing (h5sbSymbolTableEntry superblock')
        case node of
          GroupNode groupData -> pure groupData
          _ -> error "illegal h5: superblock didn't refer to a group but a dataset"
