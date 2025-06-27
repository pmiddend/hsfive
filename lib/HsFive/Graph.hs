{-# LANGUAGE BinaryLiterals #-}
{-# HLINT ignore "Use newtype instead of data" #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

module HsFive.Graph where

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
import Data.Traversable (forM)
import Data.Word (Word32)
import HsFive.Bitshuffle (DecompressResult (DecompressError, DecompressSuccess), bshufDecompressLz4)
import HsFive.CoreTypes
import Safe (headMay)
import System.File.OsPath (withBinaryFile, withFile)
import System.IO (Handle, IOMode (ReadMode, WriteMode), SeekMode (AbsoluteSeek, SeekFromEnd), hPutStrLn, hSeek, hTell)
import System.OsPath (OsPath, encodeUtf)

data GraphSymbolTableEntry = GraphSymbolTableEntry
  { gsteAddress :: Address,
    gsteName :: BSL.ByteString,
    gsteObjectHeader :: GraphObjectHeader
  }

gsteMessages :: GraphSymbolTableEntry -> [GraphMessage]
gsteMessages (GraphSymbolTableEntry _ _ (GraphObjectHeader messages)) = messages

data GraphHeap = GraphHeap [BSL.ByteString]

data GraphTree = GraphTree [GraphSymbolTableEntry]

data GraphMessage
  = GraphSymbolTableMessage Address GraphTree GraphHeap
  | GraphDataspaceMessage DataspaceMessageData
  | GraphDatatypeMessage DatatypeMessageData
  | GraphDataStorageFillValueMessage
  | GraphDataStorageFilterPipelineMessage DataStorageFilterPipelineMessageData
  | GraphDataStorageLayoutMessage DataStorageLayout
  | GraphObjectModificationTimeMessage Word32
  | GraphObjectModificationTimeOldMessage
  | GraphAttributeMessage BS.ByteString DatatypeMessageData DataspaceMessageData AttributeData
  | GraphNilMessage

data GraphObjectHeader = GraphObjectHeader [GraphMessage]

data GraphvizState = GraphvizState
  { gvCounter :: Int,
    gvDefinitions :: [String],
    gvArrows :: [String]
  }

newtype GraphvizNode = GraphvizNode String

instance Show GraphvizNode where
  show (GraphvizNode s) = s

type GraphvizStateTransformer = State GraphvizState

addNode :: String -> Maybe String -> GraphvizStateTransformer GraphvizNode
addNode prefix addition = do
  current <- get
  let newNodeName = prefix <> show (gvCounter current)
      newNode = newNodeName <> maybe ("[label=<" <> prefix <> ">]") (\addition' -> "[" <> addition' <> "]") addition
  put (current {gvCounter = gvCounter current + 1, gvDefinitions = newNode : gvDefinitions current})
  pure (GraphvizNode newNodeName)

addArrow :: GraphvizNode -> GraphvizNode -> GraphvizStateTransformer ()
addArrow (GraphvizNode from) (GraphvizNode to) = do
  current <- get
  put (current {gvArrows = (from <> " -> " <> to) : gvArrows current})

readGraphSymbolTableEntry :: Handle -> Int -> Maybe HeapWithData -> SymbolTableEntry -> IO GraphSymbolTableEntry
readGraphSymbolTableEntry handle depth maybeHeap e =
  let prefix = replicate depth ' ' <> " | "
      putStrLnWithPrefix x = putStrLn (prefix <> x)
   in do
        putStrLnWithPrefix ("at symbol table entry: " <> show e)
        let linkName =
              case maybeHeap of
                Nothing -> BSL.empty
                Just (HeapWithData _heapHeader heapData') ->
                  BSL.takeWhile (/= 0) (BSL.drop (fromIntegral (h5steLinkNameOffset e)) heapData')
        putStrLnWithPrefix $ "link name " <> show linkName
        putStrLnWithPrefix $ "seeking to " <> show (h5steObjectHeaderAddress e)
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
            putStrLnWithPrefix ("object header: " <> show header)
            putStrLnWithPrefix "iterating over messages"
            messages <-
              join
                <$> forM
                  (ohHeaderMessages header)
                  (readGraphMessage depth putStrLnWithPrefix handle prefix)
            pure (GraphSymbolTableEntry (h5steObjectHeaderAddress e) linkName (GraphObjectHeader messages))

readGraphMessage :: Int -> (String -> IO ()) -> Handle -> [Char] -> Message -> IO [GraphMessage]
readGraphMessage depth putStrLnWithPrefix handle prefix message = do
  putStrLnWithPrefix ("message: " <> show message)
  case message of
    NilMessage -> pure []
    DataspaceMessage d -> pure [GraphDataspaceMessage d]
    SymbolTableMessage btreeAddress heapAddress -> do
      putStrLnWithPrefix $ "seeking to " <> show btreeAddress <> "; check btree for symbol table"
      hSeek handle AbsoluteSeek (fromIntegral btreeAddress)
      blinkTreenode <- BSL.hGet handle 2048
      case runGetOrFail (getBLinkTreeNode Nothing Nothing) blinkTreenode of
        Left _ -> error (prefix <> "error reading b-link node")
        Right (_, _, node@(BLinkTreeNodeChunkedRawData {})) -> fail "got chunked data node inside tree"
        Right (_, _, node@(BLinkTreeNodeGroup {})) -> do
          putStrLnWithPrefix ("tree node: " <> show node)

          putStrLnWithPrefix $ "seeking to heap at " <> show heapAddress
          hSeek handle AbsoluteSeek (fromIntegral heapAddress)
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
              putStrLnWithPrefix ("heap header: " <> show heapHeader')
              putStrLnWithPrefix ("keys on heap: " <> show keysOnHeap)
              putStrLnWithPrefix ("children: " <> show childrenOnHeap)

              treeEntries <-
                mapM
                  (readGraphSymbolTableEntry handle (depth + 2) (Just (HeapWithData heapHeader' heapData')))
                  (concatMap gstnEntries childrenOnHeap)

              pure [GraphSymbolTableMessage btreeAddress (GraphTree treeEntries) (GraphHeap keysOnHeap)]
    ObjectHeaderContinuationMessage continuationAddress length' -> do
      putStrLnWithPrefix ("header continuation, seeking to " <> show continuationAddress)
      hSeek handle AbsoluteSeek (fromIntegral continuationAddress)
      data' <- BSL.hGet handle (fromIntegral length')
      let readMessage = do
            messageType <- getWord16le
            headerMessageDataSize <- getWord16le
            flags <- getWord8
            skip 3
            isolate (fromIntegral (trace ("message type " <> show messageType <> ", size " <> show headerMessageDataSize) headerMessageDataSize)) (getMessage messageType)
          decodeMessages = do
            bytesRead' <- bytesRead
            let remainder = bytesRead' `mod` 8
            skip (trace ("skipping " <> show remainder <> " byte(s) between messages") (fromIntegral remainder))
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
        Right (_, _, messages) -> do
          putStrLnWithPrefix ("decoded these messages: " <> show messages)
          join <$> forM messages (readGraphMessage depth putStrLnWithPrefix handle prefix)
    DatatypeMessage d -> pure [GraphDatatypeMessage d]
    DataStorageFillValueMessage -> pure [GraphDataStorageFillValueMessage]
    DataStorageFilterPipelineMessage d -> pure [GraphDataStorageFilterPipelineMessage d]
    DataStorageLayoutMessage d -> pure [GraphDataStorageLayoutMessage d]
    ObjectModificationTimeMessage t -> pure [GraphObjectModificationTimeMessage t]
    ObjectModificationTimeOldMessage _ _ _ _ _ _ -> pure [GraphObjectModificationTimeOldMessage]
    AttributeMessage n datatype dataspace data' -> pure [GraphAttributeMessage n datatype dataspace data']

readH5ToGraph :: OsPath -> IO GraphSymbolTableEntry
readH5ToGraph fileNameEncoded = do
  withBinaryFile fileNameEncoded ReadMode $ \handle -> do
    putStrLn "reading file"
    superblock <- readSuperblock handle

    case superblock of
      Nothing -> error "couldn't read superblock"
      Just superblock' -> do
        print superblock

        readGraphSymbolTableEntry handle 0 Nothing (h5sbSymbolTableEntry superblock')

readAndIncrement :: State Int Int
readAndIncrement = do
  current <- get
  put (current + 1)
  pure current

entryNodeToDot :: GraphvizNode -> GraphSymbolTableEntry -> State GraphvizState ()
entryNodeToDot origin (GraphSymbolTableEntry address name objectHeader) = do
  thisNode <-
    addNode
      "symbol_table_entry"
      (Just ("label=<symbol table entry<br/>" <> show name <> "<br/><i>@" <> show address <> "</i>>"))
  addArrow origin thisNode
  let objectHeaderToDot (GraphObjectHeader messages) = do
        thisOHNode <- addNode "object_header" Nothing
        addArrow thisNode thisOHNode
        mapM_ (messageToDot thisOHNode) messages
  objectHeaderToDot objectHeader

messageToDot :: GraphvizNode -> GraphMessage -> GraphvizStateTransformer ()
messageToDot origin (GraphSymbolTableMessage treeAddress (GraphTree symbolTableEntries) _heap) = do
  thisNode <-
    addNode
      "symbol_table_message"
      (Just ("label=<symbol table message<br/><i>@" <> show treeAddress <> "</i>>"))
  addArrow origin thisNode
  mapM_ (entryNodeToDot thisNode) symbolTableEntries
messageToDot origin (GraphDataspaceMessage _messageData) = do
  thisNode <- addNode "dataspace_message" Nothing
  addArrow origin thisNode
messageToDot origin (GraphDatatypeMessage _messageData) = do
  thisNode <- addNode "datatype_message" Nothing
  addArrow origin thisNode
messageToDot origin GraphDataStorageFillValueMessage = do
  thisNode <- addNode "data_storage_fill_value_message" Nothing
  addArrow origin thisNode
messageToDot origin (GraphDataStorageFilterPipelineMessage _messageData) = do
  thisNode <- addNode "data_storage_filter_pipeline_message" Nothing
  addArrow origin thisNode
messageToDot origin (GraphDataStorageLayoutMessage _messageData) = do
  thisNode <- addNode "data_storage_layout_message" Nothing
  addArrow origin thisNode
messageToDot origin (GraphObjectModificationTimeMessage _messageData) = do
  thisNode <- addNode "object_modification_time_message" Nothing
  addArrow origin thisNode
messageToDot origin (GraphAttributeMessage attributeName _ _ _) = do
  thisNode <- addNode "attribute_message" (Just ("label=<attribute " <> BS8.unpack (BS.filter (/= 0) attributeName) <> ">"))
  addArrow origin thisNode
messageToDot origin GraphNilMessage = do
  thisNode <- addNode "nil" Nothing
  addArrow origin thisNode

graphToDot :: GraphSymbolTableEntry -> String
graphToDot entry = evalState graphToDot' (GraphvizState 0 [] [])
  where
    graphToDot' = do
      rootNode <- addNode "symbol_table_entry" Nothing
      objectHeaderNode <-
        addNode
          "object_header"
          ( Just $
              "label=<object header<br/><i>@"
                <> show (gsteAddress entry)
                <> "</i>>"
          )
      mapM_ (messageToDot objectHeaderNode) (gsteMessages entry)
      let surround s = "digraph G {\n" <> s <> "\n}"
      current <- get
      pure $
        surround $
          intercalate ";\n" (gvDefinitions current)
            <> ";\n"
            <> intercalate
              ";\n"
              (["superblock -> " <> show rootNode, show rootNode <> " -> " <> show objectHeaderNode] <> gvArrows current)

data ProcessedObjectHeader = ProcessedObjectHeader
  { pohDataspace :: DataspaceMessageData,
    pohDatatype :: DatatypeMessageData,
    pohLayout :: DataStorageLayout,
    pohFilterPipeline :: Maybe DataStorageFilterPipelineMessageData
  }
  deriving (Show)

collectObjectHeaders :: GraphSymbolTableEntry -> Maybe ProcessedObjectHeader
collectObjectHeaders (GraphSymbolTableEntry {gsteObjectHeader}) =
  ProcessedObjectHeader
    <$> searchDataspace gsteObjectHeader
    <*> searchDatatype gsteObjectHeader
    <*> searchLayout gsteObjectHeader
    <*> pure (searchFilters gsteObjectHeader)
  where
    searchMessage :: GraphObjectHeader -> (GraphMessage -> Maybe a) -> Maybe a
    searchMessage (GraphObjectHeader messages) finder = headMay (mapMaybe finder messages)
    searchDataspace :: GraphObjectHeader -> Maybe DataspaceMessageData
    searchDataspace header =
      let dataspaceFinder (GraphDataspaceMessage d) = Just d
          dataspaceFinder _ = Nothing
       in searchMessage header dataspaceFinder
    searchLayout :: GraphObjectHeader -> Maybe DataStorageLayout
    searchLayout header =
      let dataspaceFinder (GraphDataStorageLayoutMessage d) = Just d
          dataspaceFinder _ = Nothing
       in searchMessage header dataspaceFinder
    searchDatatype :: GraphObjectHeader -> Maybe DatatypeMessageData
    searchDatatype header =
      let datatypeFinder (GraphDatatypeMessage d) = Just d
          datatypeFinder _ = Nothing
       in searchMessage header datatypeFinder
    searchFilters :: GraphObjectHeader -> Maybe DataStorageFilterPipelineMessageData
    searchFilters header =
      let filtersFinder (GraphDataStorageFilterPipelineMessage d) = Just d
          filtersFinder _ = Nothing
       in searchMessage header filtersFinder

readChunkedLayouts :: Handle -> GraphSymbolTableEntry -> IO ()
readChunkedLayouts handle g = mapM_ (descend Nothing) (gsteMessages g)
  where
    searchMessage :: GraphObjectHeader -> (GraphMessage -> Maybe a) -> Maybe a
    searchMessage (GraphObjectHeader messages) finder = headMay (mapMaybe finder messages)
    searchDataspace :: GraphObjectHeader -> Maybe DataspaceMessageData
    searchDataspace header =
      let dataspaceFinder (GraphDataspaceMessage d) = Just d
          dataspaceFinder _ = Nothing
       in searchMessage header dataspaceFinder
    searchDatatype :: GraphObjectHeader -> Maybe DatatypeMessageData
    searchDatatype header =
      let datatypeFinder (GraphDatatypeMessage d) = Just d
          datatypeFinder _ = Nothing
       in searchMessage header datatypeFinder
    searchFilters :: GraphObjectHeader -> Maybe DataStorageFilterPipelineMessageData
    searchFilters header =
      let filtersFinder (GraphDataStorageFilterPipelineMessage d) = Just d
          filtersFinder _ = Nothing
       in searchMessage header filtersFinder
    descend :: Maybe ProcessedObjectHeader -> GraphMessage -> IO ()
    descend _ (GraphSymbolTableMessage _ (GraphTree entries) _) = mapM_ descendToEntry entries
    descend
      ( Just
          ( ProcessedObjectHeader
              { pohDataspace,
                pohDatatype,
                pohLayout = LayoutChunked {layoutChunkedBTreeAddress}
              }
            )
        )
      _ =
        case layoutChunkedBTreeAddress of
          Nothing -> pure ()
          Just addr -> do
            hSeek handle AbsoluteSeek (fromIntegral addr)
            data' <- BSL.hGet handle 2048
            case runGetOrFail (getBLinkTreeNode (Just pohDataspace) (Just pohDatatype)) data' of
              Left (_, bytesConsumed, e') ->
                error
                  ( "invalid b tree node (consumed "
                      <> show bytesConsumed
                      <> " bytes): "
                      <> show e'
                  )
              Right (_, _, node@(BLinkTreeNodeChunkedRawData {bltnChunks})) -> do
                putStrLn ("tree node: " <> show node)
                let readSingleChunk :: ChunkInfo Address -> IO ()
                    readSingleChunk ci = do
                      putStrLn ("seeking to " <> show (ciChunkPointer ci))
                      hSeek handle AbsoluteSeek (fromIntegral (ciChunkPointer ci))
                      chunkData <- BSL.hGet handle (fromIntegral (ciSize ci))
                      case datatypeClass pohDatatype of
                        (DatatypeFixedPoint {fixedPointDataElementSize, fixedPointByteOrder}) ->
                          case dataspaceDimensions pohDataspace of
                            [DataspaceDimension {ddSize}] ->
                              if fixedPointDataElementSize == 8 && fixedPointByteOrder == LittleEndian
                                then case runGetOrFail (replicateM (fromIntegral ddSize) getInt64le) chunkData of
                                  Left (_, bytesConsumed, e') ->
                                    error
                                      ( "invalid chunk (consumed "
                                          <> show bytesConsumed
                                          <> " bytes): "
                                          <> show e'
                                      )
                                  Right (_, _, numbers) -> do
                                    putStrLn ("read numbers " <> show numbers)
                                else putStrLn "chunk isn't 8 byte little endian"
                            dimensions -> putStrLn ("unsupported dimensions " <> show dimensions)
                        dt -> putStrLn ("invalid data type (not fixed point or not there) " <> show dt)
                mapM_ readSingleChunk bltnChunks
              Right (_, _, _) -> error "got an inner node but expected a chunk"
    descend _ _ = pure ()
    descendToEntry :: GraphSymbolTableEntry -> IO ()
    descendToEntry e = mapM_ (descend (collectObjectHeaders e)) (gsteMessages e)

newtype Indent = Indent Int

increaseIndent :: Indent -> Indent
increaseIndent (Indent n) = Indent (n + 3)

paddingToH5Dump :: StringPadding -> String
paddingToH5Dump PaddingNullTerminate = "H5T_STR_NULLTERM"
paddingToH5Dump PaddingNull = "H5T_STR_NULLPAD"
paddingToH5Dump p = error ("invalid padding " <> show p)

charsetToH5Dump :: CharacterSet -> String
charsetToH5Dump CharacterSetAscii = "H5T_CSET_ASCII"
charsetToH5Dump CharacterSetUtf8 = "H5T_CSET_UTF8"

stringSize :: DataspaceMessageData -> BSL8.ByteString -> Int64
stringSize (DataspaceMessageData {dataspaceDimensions}) s = case dataspaceDimensions of
  [] -> BSL.length (BSL.takeWhile (/= 0) s)
  [DataspaceDimension {ddSize}] -> BSL.length (BSL.takeWhile (/= 0) s) `div` fromIntegral ddSize
  _ -> error ("got string with dataspace dimensions " <> show dataspaceDimensions)

dataspaceToH5Dump :: Indent -> DataspaceMessageData -> [(Indent, String)]
dataspaceToH5Dump m (DataspaceMessageData {dataspaceDimensions}) =
  case dataspaceDimensions of
    [] -> [(m, "DATASPACE  SCALAR")]
    dims ->
      let inner = intercalate ", " (show . ddSize <$> dims)
       in [(m, "DATASPACE  SIMPLE { ( " <> inner <> " ) / ( " <> inner <> " ) }")]

datatypeToH5Dump :: Indent -> ProcessedObjectHeader -> [(Indent, String)]
datatypeToH5Dump m (ProcessedObjectHeader {pohDatatype = DatatypeMessageData {datatypeClass = DatatypeVariableLengthString padding charset _word}}) =
  [ (m, "DATATYPE  H5T_STRING {"),
    (increaseIndent m, "STRSIZE H5T_VARIABLE;"),
    (increaseIndent m, "STRPAD " <> paddingToH5Dump padding <> ";"),
    (increaseIndent m, "CSET " <> charsetToH5Dump charset <> ";"),
    (increaseIndent m, "CTYPE H5T_C_S1;"),
    (m, "}")
  ]
datatypeToH5Dump m (ProcessedObjectHeader {pohDatatype = DatatypeMessageData {datatypeClass = DatatypeString padding charset}, pohLayout = LayoutContiguous {layoutContiguousSize}}) =
  [ (m, "DATATYPE  H5T_STRING {"),
    (increaseIndent m, "STRSIZE " <> show layoutContiguousSize <> ";"),
    (increaseIndent m, "STRPAD " <> paddingToH5Dump padding <> ";"),
    (increaseIndent m, "CSET " <> charsetToH5Dump charset <> ";"),
    (increaseIndent m, "CTYPE H5T_C_S1;"),
    (m, "}")
  ]
datatypeToH5Dump m (ProcessedObjectHeader {pohDatatype = DatatypeMessageData {datatypeClass = DatatypeFloatingPoint {floatingPointByteOrder = LittleEndian, floatingPointBitPrecision = 64}}}) = [(m, "DATATYPE  H5T_IEEE_F64LE")]
datatypeToH5Dump m (ProcessedObjectHeader {pohDatatype = DatatypeMessageData {datatypeClass = DatatypeFloatingPoint {floatingPointByteOrder = LittleEndian, floatingPointBitPrecision = 32}}}) = [(m, "DATATYPE  H5T_IEEE_F32LE")]
datatypeToH5Dump m (ProcessedObjectHeader {pohDatatype = DatatypeMessageData {datatypeClass = DatatypeFixedPoint {fixedPointByteOrder = LittleEndian, fixedPointBitPrecision = 32, fixedPointSigned = True}}}) = [(m, "DATATYPE  H5T_STD_I32LE")]
datatypeToH5Dump m (ProcessedObjectHeader {pohDatatype = DatatypeMessageData {datatypeClass = DatatypeFixedPoint {fixedPointByteOrder = LittleEndian, fixedPointBitPrecision = 64, fixedPointSigned = True}}}) = [(m, "DATATYPE  H5T_STD_I64LE")]
datatypeToH5Dump m (ProcessedObjectHeader {pohDatatype = DatatypeMessageData {datatypeClass = DatatypeFixedPoint {fixedPointByteOrder = LittleEndian, fixedPointBitPrecision = 16, fixedPointSigned = False}}}) = [(m, "DATATYPE  H5T_STD_U16LE")]
datatypeToH5Dump m t = [(m, ("FIXME " <> show t))]

graphSymbolTableEntryToH5Dump :: Indent -> GraphSymbolTableEntry -> [(Indent, String)]
graphSymbolTableEntryToH5Dump n gste@(GraphSymbolTableEntry {gsteObjectHeader = GraphObjectHeader messages, gsteName}) =
  let attributeDatatypeToH5Dump :: Indent -> Datatype -> DataspaceMessageData -> AttributeData -> [(Indent, String)]
      attributeDatatypeToH5Dump m (DatatypeVariableLengthString padding charset _word) _ _ =
        [ (m, "DATATYPE  H5T_STRING {"),
          (increaseIndent m, "STRSIZE H5T_VARIABLE;"),
          (increaseIndent m, "STRPAD " <> paddingToH5Dump padding <> ";"),
          (increaseIndent m, "CSET " <> charsetToH5Dump charset <> ";"),
          (increaseIndent m, "CTYPE H5T_C_S1;"),
          (m, "}")
        ]
      attributeDatatypeToH5Dump m (DatatypeString padding charset) dataspaceMessageData (AttributeDataContent s) =
        [ (m, "DATATYPE  H5T_STRING {"),
          (increaseIndent m, "STRSIZE " <> show (stringSize dataspaceMessageData s) <> ";"),
          (increaseIndent m, "STRPAD " <> paddingToH5Dump padding <> ";"),
          (increaseIndent m, "CSET " <> charsetToH5Dump charset <> ";"),
          (increaseIndent m, "CTYPE H5T_C_S1;"),
          (m, "}")
        ]
      attributeDatatypeToH5Dump _ e _ _ = error ("add this datatype: " <> show e)

      attributeMessageToH5Dump :: GraphMessage -> [(Indent, String)]
      attributeMessageToH5Dump (GraphAttributeMessage name (DatatypeMessageData _ datatype) dataspace data') =
        [ (increaseIndent n, "ATTRIBUTE \"" <> BS8.unpack (BS.filter (/= 0) name) <> "\" {")
        ]
          <> attributeDatatypeToH5Dump (increaseIndent (increaseIndent n)) datatype dataspace data'
          <> dataspaceToH5Dump (increaseIndent (increaseIndent n)) dataspace
          <> [(increaseIndent n, "}")]
      attributeMessageToH5Dump _ = []
      attributeMessageName :: GraphMessage -> String
      attributeMessageName (GraphAttributeMessage name _ _ _) = BS8.unpack name
      attributeMessageName _ = ""
      symbolTableMessageToH5Dump :: GraphMessage -> [(Indent, String)]
      symbolTableMessageToH5Dump (GraphSymbolTableMessage _address (GraphTree es) _heap) = es >>= graphSymbolTableEntryToH5Dump (increaseIndent n)
      symbolTableMessageToH5Dump _ = []
      possiblyDataspace =
        case collectObjectHeaders gste of
          Nothing -> Nothing
          Just poh@(ProcessedObjectHeader {pohDataspace}) ->
            Just $ \insideDataset ->
              [(n, "DATASET \"" <> BSL8.unpack gsteName <> "\" {")]
                <> datatypeToH5Dump (increaseIndent n) poh
                <> dataspaceToH5Dump (increaseIndent n) pohDataspace
                <> insideDataset
                <> [ (n, "}")
                   ]
      attributes = sortOn attributeMessageName messages >>= attributeMessageToH5Dump
      subgroups = messages >>= symbolTableMessageToH5Dump
      groupName = if BSL.null gsteName then "/" else BSL8.unpack (BSL.filter (/= 0) gsteName)
   in case possiblyDataspace of
        Nothing -> [(n, "GROUP \"" <> groupName <> "\" {")] <> attributes <> subgroups <> [(n, "}")]
        Just ds -> ds (attributes <> subgroups)

graphToH5Dump :: GraphSymbolTableEntry -> String
graphToH5Dump gste =
  let showDump :: [(Indent, String)] -> String
      showDump lines' = intercalate "\n" ((\(Indent indent, line) -> replicate indent ' ' <> line) <$> lines')
   in showDump (graphSymbolTableEntryToH5Dump (Indent 0) gste)
