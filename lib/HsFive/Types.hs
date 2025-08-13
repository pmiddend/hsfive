{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module HsFive.Types where

import Control.Applicative ((<|>))
import Control.Monad (MonadPlus, replicateM)
import Data.Binary (getWord8)
import Data.Binary.Get (bytesRead, getInt32le, getInt64le, getWord16le, isEmpty, isolate, label, runGet, runGetOrFail, skip)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Foldable (find, msum)
import Data.IORef (IORef, modifyIORef, newIORef, readIORef)
import Data.List (singleton)
import qualified Data.List.NonEmpty as NE
import Data.List.Split (chunksOf)
import qualified Data.Map.Strict as Map
import Data.Maybe (mapMaybe)
import qualified Data.Set as Set
import Data.Text (Text, intercalate, null, pack, unpack)
import Data.Text.Encoding (decodeUtf8Lenient)
import Data.Word (Word64)
import HsFive.Bitshuffle (DecompressResult (DecompressError, DecompressSuccess, decompressBytes), bshufDecompressLz4)
import HsFive.CoreTypes
  ( Address,
    AttributeContent (AttributeContentCompound, AttributeContentEnumeration, AttributeContentFixedString, AttributeContentFloating, AttributeContentIntegral, AttributeContentReference, AttributeContentTodo, AttributeContentVariableLengthSequence, AttributeContentVariableString),
    AttributeContentCompoundMember (AttributeContentCompoundMember),
    BLinkTreeNode (BLinkTreeNodeChunkedRawData, BLinkTreeNodeGroup, bltnChunks),
    ByteOrder (LittleEndian),
    ChunkInfo (ciChunkPointer, ciSize),
    DataStorageFilterPipelineMessageData (DataStorageFilterPipelineMessageData, dataStorageFilterPipelineFilters),
    DataStorageLayout,
    DataStoragePipelineFilter (DataStoragePipelineFilter),
    DataspaceDimension (ddSize),
    DataspaceMessageData (DataspaceMessageData, dataspaceDimensions, dataspacePermutationIndices),
    Datatype (DatatypeFixedPoint, fixedPointByteOrder, fixedPointDataElementSize),
    DatatypeMessageData (DatatypeMessageData, datatypeClass),
    EnumerationMap,
    GlobalHeap (globalHeapObjects),
    GlobalHeapObject (GlobalHeapObject, globalHeapObjectData, globalHeapObjectIndex),
    GroupSymbolTableNode (GroupSymbolTableNode, gstnVersion),
    HeapWithData (HeapWithData),
    Length,
    LinkMessageData (LinkMessageData, linkInfoLinkType),
    LinkType (HardLink),
    Message (AttributeMessage, DataStorageFilterPipelineMessage, DataStorageLayoutMessage, DataspaceMessage, DatatypeMessage, LinkMessage, ObjectHeaderContinuationMessage, SharedMessage, SymbolTableMessage),
    ObjectHeader (ObjectHeader, ohTrackCreationOrder),
    ReferenceType,
    SharedMessageData (SharedMessageData),
    SharedMessageType (SharedMessageTypeCommitted),
    Superblock,
    SymbolTableEntry (h5steLinkNameOffset),
    SymbolTableMessageData (SymbolTableMessageData, symbolTableMessageLocalHeapAddress, symbolTableMessageV1BTreeAddress),
    bltnChildPointers,
    bltnKeyOffsets,
    dataStoragePipelineFilterClientDataValues,
    dataStoragePipelineFilterId,
    debugLog,
    getBLinkTreeNode,
    getGlobalHeap,
    getGroupSymbolTableNode,
    getHeapHeader,
    getMessage,
    getObjectHeader,
    getSuperblock,
    getV2ObjectHeaderContinuationBlock,
    gstnEntries,
    h5sbSymbolTableEntry,
    h5steObjectHeaderAddress,
    heapDataSegmentAddress,
    heapDataSegmentSize,
    ohHeaderMessages,
    readKey,
  )
import qualified HsFive.CoreTypes as CoreTypes
import Safe (headMay)
import System.File.OsPath (withBinaryFile, withFile)
import System.IO (Handle, IOMode (ReadMode, WriteMode), SeekMode (AbsoluteSeek, SeekFromEnd), hPutStrLn, hSeek, hTell)
import System.OsPath (OsPath, encodeUtf)
import Prelude hiding (null, unwords)

newtype Path = Path [Text] deriving (Eq)

instance Show Path where
  show (Path []) = "/"
  show (Path components) = "/" <> unpack (intercalate "/" components)

singletonPath :: Text -> Path
singletonPath t = Path [t]

unwrapPath :: Path -> a -> (NE.NonEmpty Text -> a) -> a
unwrapPath (Path []) whenEmpty _whenFull = whenEmpty
unwrapPath (Path (x : xs)) _whenEmpty whenFull = whenFull (x NE.:| xs)

pathComponentsList :: Path -> [Text]
pathComponentsList (Path p) = p

data AttributeData
  = AttributeDataString !Text
  | AttributeDataIntegral !Integer
  | AttributeDataFloating !Double
  | AttributeDataReference !ReferenceType !BS.ByteString
  | AttributeDataEnumeration !EnumerationMap !Int
  | AttributeDataVariableLengthRaw !BS.ByteString
  | AttributeDataCompound ![(Text, AttributeData)]
  deriving (Show)

data Attribute = Attribute
  { attributeName :: !Text,
    attributeType :: !Datatype,
    attributeDimensions :: ![DataspaceDimension],
    attributePermutationIndices :: ![Length],
    attributeData :: !AttributeData
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
  | DatatypeNode !Datatype
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

resolveContinuationMessages :: Handle -> Bool -> [Message] -> IO [Message]
resolveContinuationMessages handle trackCreationOrder messages = do
  putStrLn "resolving continuation"
  foldMap (resolveContinuationMessage handle trackCreationOrder) messages

resolveSharedMessages :: Handle -> [Message] -> IO [Message]
resolveSharedMessages handle messages = do
  putStrLn "resolving shared messages"
  foldMap (resolveSharedMessage handle) messages

resolveHardLinks :: Handle -> IORef NodeReaderState -> Maybe Path -> Maybe (HeapWithData, Word64) -> [Message] -> IO [Node]
resolveHardLinks handle readerStateRef previousPath maybeHeap messages = do
  putStrLn "resolving hard links"
  foldMap (resolveHardLink handle readerStateRef previousPath maybeHeap) messages

resolveHardLink :: Handle -> IORef NodeReaderState -> Maybe Path -> Maybe (HeapWithData, Word64) -> Message -> IO [Node]
resolveHardLink handle readerStateRef previousPath maybeHeap (LinkMessage (LinkMessageData {linkInfoLinkType = HardLink linkAddress})) =
  singleton <$> readNode handle readerStateRef previousPath maybeHeap linkAddress
-- putStrLn $ "seeking to hard link " <> show linkAddress
-- hSeek handle AbsoluteSeek (fromIntegral linkAddress)
-- objectHeaderData <- BSL.hGet handle 4096
-- case runGetOrFail getObjectHeaderV1 objectHeaderData of
--   Left (_, bytesConsumed, e') ->
--     error
--       ( "invalid object header at symbol table entry (consumed "
--           <> show bytesConsumed
--           <> " bytes): "
--           <> show e'
--       )
--   Right (_, _, header) -> do
resolveHardLink _handle _readerStateref _previousPath _maybeHeap _message = pure []

resolveSharedMessage :: Handle -> Message -> IO [Message]
resolveSharedMessage handle (SharedMessage (SharedMessageData _originalType (SharedMessageTypeCommitted otherHeaderAddress))) = do
  putStrLn $ "shared seeking to " <> show otherHeaderAddress
  hSeek handle AbsoluteSeek (fromIntegral otherHeaderAddress)
  data' <- BSL.hGet handle 4096
  case runGetOrFail getObjectHeader data' of
    Left (_, _, e') -> error ("parsing shared message target (object header) failed: " <> show e')
    Right (_, _, ObjectHeader {ohHeaderMessages = [message]}) -> do
      putStrLn $ "continuation message: " <> show message
      pure [message]
    Right (_, _, otherHeader) -> error ("parsing shared message target (object header) failed, expected exactly one message in the shared message object header, but got: " <> show otherHeader)
resolveSharedMessage _handle m = pure [m]

resolveContinuationMessage :: Handle -> Bool -> Message -> IO [Message]
resolveContinuationMessage handle trackCreationOrder (ObjectHeaderContinuationMessage continuationAddress length') = do
  putStrLn $ "continuation seeking to " <> show continuationAddress
  hSeek handle AbsoluteSeek (fromIntegral continuationAddress)
  data' <- BSL.hGet handle (fromIntegral length')
  -- FIXME: This is a duplicate from CoreTypes (almost), put all Binary stuff in there.
  let readMessage = do
        messageType <- getWord16le
        headerMessageDataSize <- getWord16le
        _flags <- getWord8
        skip 3
        label ("message type " <> show messageType) (isolate (fromIntegral headerMessageDataSize) (getMessage messageType))
      decodeV2Messages = getV2ObjectHeaderContinuationBlock trackCreationOrder
      decodeV1Messages = do
        bytesRead' <- bytesRead
        let remainder = bytesRead' `mod` 8
        skip (fromIntegral remainder)
        -- skip (fromIntegral remainder)
        empty <- isEmpty
        if empty
          then pure []
          else do
            m <- readMessage
            ms <- decodeV1Messages
            pure (debugLog "m" m : ms)
  case runGetOrFail (decodeV2Messages <|> decodeV1Messages) data' of
    Left (_, _, e') -> error ("parsing continuation messages failed: " <> show e')
    Right (_, _, messages) -> do
      putStrLn $ "continuation messages: " <> show messages
      -- Important: We could have more continuations, recursively!
      foldMap (resolveContinuationMessage handle trackCreationOrder) messages
resolveContinuationMessage _handle _trackCreationOrder m = pure [m]

appendPath :: Path -> Text -> Path
appendPath (Path xs) x
  | null x = error "empty path component not allowed"
  | otherwise = Path (xs <> [x])

(</) :: Path -> Text -> Path
(</) = appendPath

-- Same as FilePath operator
infixl 5 </

rootPath :: Path
rootPath = Path mempty

convertAttributeContent :: Handle -> CoreTypes.AttributeContent -> IO AttributeData
convertAttributeContent _handle (AttributeContentFixedString content) = pure $ AttributeDataString (decodeUtf8Lenient $ BS.takeWhile (/= 0) content)
convertAttributeContent _handle (AttributeContentEnumeration enumerationMap enumerationValue) = pure $ AttributeDataEnumeration enumerationMap enumerationValue
convertAttributeContent handle (AttributeContentVariableLengthSequence _baseType heapAddress objectIndex _size) = do
  putStrLn $ "variable-length sequence: seeking to heap at " <> show heapAddress
  hSeek handle AbsoluteSeek (fromIntegral heapAddress)
  heapData <- BSL.hGet handle 8192
  case runGetOrFail getGlobalHeap heapData of
    Left (_, bytesConsumed, e') ->
      error
        ( "invalid global heap (consumed "
            <> show bytesConsumed
            <> " bytes): "
            <> show e'
        )
    Right (_, _, globalHeap) -> do
      case find (\ho -> globalHeapObjectIndex ho == fromIntegral objectIndex) (globalHeapObjects globalHeap) of
        Nothing -> error ("cannot find variable-length object " <> show objectIndex <> " in global heap")
        Just (GlobalHeapObject {globalHeapObjectData}) ->
          pure (AttributeDataVariableLengthRaw globalHeapObjectData)
convertAttributeContent handle (AttributeContentVariableString heapAddress objectIndex _size _charset) = do
  -- TODO: apply heap cache here?
  putStrLn $ "variable string: seeking to heap at " <> show heapAddress
  hSeek handle AbsoluteSeek (fromIntegral heapAddress)
  heapData <- BSL.hGet handle 8192
  case runGetOrFail getGlobalHeap heapData of
    Left (_, bytesConsumed, e') ->
      error
        ( "invalid global heap (consumed "
            <> show bytesConsumed
            <> " bytes): "
            <> show e'
        )
    Right (_, _, globalHeap) -> do
      case find (\ho -> globalHeapObjectIndex ho == fromIntegral objectIndex) (globalHeapObjects globalHeap) of
        Nothing -> error ("cannot find object " <> show objectIndex <> " in global heap")
        Just (GlobalHeapObject {globalHeapObjectData}) ->
          pure $ AttributeDataString (decodeUtf8Lenient $ BS.takeWhile (/= 0) globalHeapObjectData)
convertAttributeContent _handle (AttributeContentIntegral number) = pure $ AttributeDataIntegral number
convertAttributeContent _handle (AttributeContentFloating number) = pure $ AttributeDataFloating number
convertAttributeContent _handle (AttributeContentReference referenceType content) = pure $ AttributeDataReference referenceType content
convertAttributeContent handle (AttributeContentCompound members) = do
  let convertWithName (AttributeContentCompoundMember name content) = do
        convertedContent <- convertAttributeContent handle content
        pure (decodeUtf8Lenient name, convertedContent)
  convertedMembers <- traverse convertWithName members
  pure (AttributeDataCompound convertedMembers)
convertAttributeContent _handle (AttributeContentTodo realType content) = error $ "invalid attribute data, not a known attribute type: " <> show realType <> ", bytes: " <> show (BSL.unpack content)

convertAttribute :: Handle -> CoreTypes.AttributeData -> IO Attribute
convertAttribute
  handle
  ( CoreTypes.AttributeData
      { CoreTypes.attributeName = an,
        CoreTypes.attributeDatatypeMessageData = DatatypeMessageData {datatypeClass = typeData},
        CoreTypes.attributeDataspaceMessageData = DataspaceMessageData {dataspaceDimensions, dataspacePermutationIndices},
        CoreTypes.attributeContent = content
      }
    ) = do
    contentConverted <- convertAttributeContent handle content
    pure
      Attribute
        { attributeName = decodeUtf8Lenient $ BS.takeWhile (/= 0) an,
          attributeType = typeData,
          attributeDimensions = dataspaceDimensions,
          attributePermutationIndices = dataspacePermutationIndices,
          attributeData = contentConverted
        }

readNode :: Handle -> IORef NodeReaderState -> Maybe Path -> Maybe (HeapWithData, Word64) -> Address -> IO Node
readNode handle readerStateRef previousPath maybeHeap e = do
  let updateVisiting a = do
        putStrLn $ "adding " <> show e <> " to visiting nodes"
        modifyIORef
          readerStateRef
          ( \s -> s {nodeReaderStateVisiting = Set.insert a (nodeReaderStateVisiting s)}
          )

  readerState <- readIORef readerStateRef
  updateVisiting e
  putStrLn $ "node seeking to " <> show e
  hSeek handle AbsoluteSeek (fromIntegral e)
  objectHeaderData <- BSL.hGet handle 4096
  case runGetOrFail getObjectHeader objectHeaderData of
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
              Nothing -> rootPath
              Just (HeapWithData _heapHeader heapData'', linkNameOffset) ->
                case previousPath of
                  Nothing -> rootPath
                  Just previousPath' ->
                    appendPath
                      previousPath'
                      ( decodeUtf8Lenient $
                          BSL.toStrict $
                            BSL.takeWhile (/= 0) $
                              BSL.drop (fromIntegral linkNameOffset) heapData''
                      )
      allMessagesAfterContinuations <-
        resolveContinuationMessages
          handle
          (ohTrackCreationOrder header)
          (ohHeaderMessages header)
      allMessages <- resolveSharedMessages handle allMessagesAfterContinuations
      let filterAttribute (AttributeMessage attributeData) = Just attributeData
          filterAttribute _ = Nothing
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
          case searchMessage filterDatatype of
            Nothing -> do
              hardLinkNodes <- resolveHardLinks handle readerStateRef (Just newPath) maybeHeap allMessages
              case hardLinkNodes of
                [] -> fail ("dataset without datatype and without any hardlinks; all messages: " <> show allMessages)
                hardlinksNonEmpty -> do
                  pure $
                    GroupNode $
                      GroupData
                        { groupPath = newPath,
                          groupAttributes = [],
                          groupChildren = hardlinksNonEmpty
                        }
            Just (DatatypeMessageData {datatypeClass}) ->
              case searchMessage filterDataspace of
                Nothing -> pure (DatatypeNode datatypeClass)
                -- fail ("dataset without dataspace: " <> show allMessages)
                Just (DataspaceMessageData {dataspaceDimensions, dataspacePermutationIndices}) ->
                  let filters = case searchMessage filterFilters of
                        Nothing -> []
                        Just (DataStorageFilterPipelineMessageData {dataStorageFilterPipelineFilters}) -> dataStorageFilterPipelineFilters
                   in case searchMessage filterLayout of
                        Nothing -> fail "dataset without layout"
                        Just layout -> do
                          attributes <- mapM (convertAttribute handle) (mapMaybe filterAttribute allMessages)
                          pure $
                            DatasetNode $
                              DatasetData
                                { datasetDimensions = dataspaceDimensions,
                                  datasetPermutationIndices = dataspacePermutationIndices,
                                  datasetDatatype = datatypeClass,
                                  datasetFilters = filters,
                                  datasetStorageLayout = layout,
                                  datasetPath = newPath,
                                  datasetAttributes = attributes
                                }
        Just (SymbolTableMessageData {symbolTableMessageV1BTreeAddress, symbolTableMessageLocalHeapAddress}) -> do
          putStrLn $ "seeking to btree " <> show symbolTableMessageV1BTreeAddress
          hSeek handle AbsoluteSeek (fromIntegral symbolTableMessageV1BTreeAddress)
          blinkTreenode <- BSL.hGet handle 2048
          case runGetOrFail (getBLinkTreeNode Nothing) blinkTreenode of
            Left _ -> error "error reading b-link node"
            Right (_, _, BLinkTreeNodeChunkedRawData {}) -> fail "got chunked data node inside tree"
            Right (_, _, node@(BLinkTreeNodeGroup {})) -> do
              putStrLn $ "seeking to local heap " <> show symbolTableMessageLocalHeapAddress
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
                      readChild :: Address -> IO (GroupSymbolTableNode Address)
                      readChild addr = do
                        hSeek handle AbsoluteSeek (fromIntegral addr)
                        rawData <- BSL.hGet handle 2048
                        let result = runGet getGroupSymbolTableNode rawData
                            resultWithoutInvalidAddresses =
                              GroupSymbolTableNode
                                { gstnVersion = gstnVersion result,
                                  gstnEntries =
                                    mapMaybe
                                      ( \entry ->
                                          case h5steObjectHeaderAddress entry of
                                            Nothing -> Nothing
                                            Just address' ->
                                              if Set.member address' (nodeReaderStateVisiting readerState)
                                                then Nothing
                                                else Just (entry {h5steObjectHeaderAddress = address'})
                                      )
                                      (gstnEntries result)
                                }
                        pure resultWithoutInvalidAddresses
                  _keysOnHeap <- mapM (readKey handle) keyAddressesOnHeap
                  childrenOnHeap <- mapM readChild childAddressesOnHeap

                  putStrLn $ "children on heap: " <> show (concatMap gstnEntries childrenOnHeap)

                  childNodes <-
                    mapM
                      ( \entry ->
                          readNode
                            handle
                            readerStateRef
                            (Just newPath)
                            (Just (HeapWithData heapHeader' heapData', h5steLinkNameOffset entry))
                            (h5steObjectHeaderAddress entry)
                      )
                      (concatMap gstnEntries childrenOnHeap)

                  attributes <- mapM (convertAttribute handle) (mapMaybe filterAttribute allMessages)

                  pure $
                    GroupNode $
                      GroupData
                        { groupPath = newPath,
                          groupAttributes = attributes,
                          groupChildren = childNodes
                        }

data NodeReaderState = NodeReaderState
  { nodeReaderStateNodeCache :: Map.Map Address Node,
    nodeReaderStateVisiting :: Set.Set Address
  }

readH5 :: OsPath -> IO GroupData
readH5 fileNameEncoded = do
  withBinaryFile fileNameEncoded ReadMode $ \handle -> do
    superblock <- readSuperblock handle

    case superblock of
      Nothing -> error "couldn't read superblock"
      Just superblock' -> do
        readerState <- newIORef (NodeReaderState mempty mempty)
        node <- readNode handle readerState Nothing Nothing (h5steObjectHeaderAddress (h5sbSymbolTableEntry superblock'))
        case node of
          GroupNode groupData -> pure groupData
          _ -> error "illegal h5: superblock didn't refer to a group but a dataset"

goToNode :: GroupData -> Path -> Maybe Node
goToNode gd p = goToNode' (GroupNode gd) (pathComponentsList p)
  where
    goToNode' :: Node -> [Text] -> Maybe Node
    goToNode' n [] = Just n
    goToNode' n@(GroupNode (GroupData {groupPath, groupChildren})) allPath@(x : xs) =
      unwrapPath
        groupPath
        (tryPick (`goToNode'` allPath) groupChildren)
        ( \nonEmptyPath ->
            if NE.last nonEmptyPath == x
              then
                if xs == mempty
                  then Just n
                  else tryPick (`goToNode'` xs) groupChildren
              else Nothing
        )
    goToNode' n@(DatasetNode (DatasetData {datasetPath})) [x] =
      unwrapPath
        datasetPath
        (error "a dataset with a root path encountered")
        (\nonEmptyPath -> if NE.last nonEmptyPath == x then Just n else Nothing)
    goToNode' (DatasetNode {}) _ = Nothing
    goToNode' (DatatypeNode {}) _ = Nothing

applyFilters :: BSL.ByteString -> [DataStoragePipelineFilter] -> IO BSL.ByteString
applyFilters data' [DataStoragePipelineFilter {dataStoragePipelineFilterId = 32008, dataStoragePipelineFilterClientDataValues = [_, _, elementSize, _blockSize, 2]}] =
  let dataStrict = BSL.toStrict data'
   in case bshufDecompressLz4 dataStrict (fromIntegral elementSize) of
        DecompressError errorCode -> error ("bitshuffle decompression error: " <> show errorCode)
        DecompressSuccess {decompressBytes} -> pure (BSL.fromStrict decompressBytes)
applyFilters _data_ unknownFilter = error ("unknown filter: " <> show unknownFilter)

showText :: (Show a) => a -> Text
showText = pack . show

readSingleChunk :: Handle -> Datatype -> Int -> [DataStoragePipelineFilter] -> [DataspaceDimension] -> ChunkInfo Address -> IO ()
readSingleChunk handle datatype chunkElementCount filters dimensions ci = do
  hSeek handle AbsoluteSeek (fromIntegral (ciChunkPointer ci))
  chunkData <- BSL.hGet handle (fromIntegral (ciSize ci))
  case datatype of
    (DatatypeFixedPoint {fixedPointDataElementSize, fixedPointByteOrder}) ->
      case (fixedPointDataElementSize, fixedPointByteOrder) of
        (8, LittleEndian) ->
          case runGetOrFail (replicateM (fromIntegral (ciSize ci)) getInt64le) chunkData of
            Left (_, bytesConsumed, e') ->
              error
                ( "invalid chunk (consumed "
                    <> show bytesConsumed
                    <> " bytes): "
                    <> show e'
                )
            Right (_, _, numbers) -> do
              putStrLn ("read numbers " <> show numbers)
        (4, LittleEndian) -> do
          chunkDataFiltered <- applyFilters chunkData filters
          case runGetOrFail (replicateM (fromIntegral chunkElementCount) getInt32le) chunkDataFiltered of
            Left (_, bytesConsumed, e') ->
              error
                ( "invalid chunk word32le chunk (consumed "
                    <> show bytesConsumed
                    <> " bytes): "
                    <> show e'
                )
            Right (_, _, numbers) -> do
              case ddSize <$> dimensions of
                [_imageCount, width, height] -> do
                  pgmPath <- encodeUtf "/tmp/test.pgm"
                  withFile pgmPath WriteMode $ \pgmHandle -> do
                    hPutStrLn pgmHandle "P2"
                    hPutStrLn pgmHandle $ show height <> " " <> show width
                    hPutStrLn pgmHandle "1300"
                    mapM_ (hPutStrLn pgmHandle . unpack) (intercalate "  " . (showText <$>) <$> chunksOf (fromIntegral width) numbers)
                _ -> error "invalid dimensions for chunk"
        _ -> putStrLn "chunk isn't 4/8 byte little endian"
    dt -> putStrLn ("invalid data type (not fixed point or not there) " <> show dt)

readChunkInfos :: (Integral a) => Handle -> a -> [DataspaceDimension] -> IO [ChunkInfo Address]
readChunkInfos handle btreeAddress dataspaceDimensions = do
  hSeek handle AbsoluteSeek (fromIntegral btreeAddress)
  data' <- BSL.hGet handle 2048
  case runGetOrFail (getBLinkTreeNode (Just dataspaceDimensions)) data' of
    Left (_, bytesConsumed, e') ->
      error
        ( "invalid b tree node (consumed "
            <> show bytesConsumed
            <> " bytes): "
            <> show e'
        )
    Right (_, _, BLinkTreeNodeChunkedRawData {bltnChunks}) -> pure bltnChunks
    Right (_, _, _) -> error "got an inner node but expected a chunk"
