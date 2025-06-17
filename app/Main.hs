{-# LANGUAGE BinaryLiterals #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use newtype instead of data" #-}

module Main where

-- import Debug.Trace (trace, traceShowId, traceWith)
import Control.Monad.State (State, evalState, get, put)
import Data.Binary (getWord8)
import Data.Binary.Get (bytesRead, getWord16le, isEmpty, isolate, runGet, runGetOrFail, skip)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.List (intercalate)
import Data.Traversable (forM)
import Data.Word (Word32)
import HsFive.CoreTypes
import System.File.OsPath (withBinaryFile)
import System.IO (Handle, IOMode (ReadMode), SeekMode (AbsoluteSeek, SeekFromEnd), hSeek, hTell)
import System.OsPath (OsPath, encodeUtf)

trace :: String -> a -> a
trace x f = f

traceWith :: (a -> String) -> a -> a
traceWith x f = f

{-
Graphviz:
digraph G {
    object_header_0[label=<object header 0<br/>@96>];
    btree_1[label=<btree 1<br />@136>];
    heap_1[label=<heap 1<br />@680>];
    btree_2[label=<btree 2<br />@840>];
    heap_2[label=<heap 2<br />@1384>];
    object_header_1[label=<object header 1<br/>@800>];
    attribute_message_1[label=<attribute message 1<br/><i>NX_class</i>>];
    group_symbol_table_1[label=<group symbol table 1<br/><i>entry</i>>];
    group_symbol_table_2[label=<group symbol table 2<br/><i>data</i>>];
    object_header_2[label=<object header 2<br/>@1960>];
    attribute_message_2[label=<attribute message 2<br/><i>NX_class</i>>];
    btree_3[label=<btree 3<br />@6144>];
    heap_3[label=<heap 3<br />@6688>];

    superblock -> symbol_table_entry_0;
    symbol_table_entry_0 -> object_header_0;
    object_header_0 -> symbol_table_message_1;
    symbol_table_message_1 -> btree_1;
    symbol_table_message_1 -> heap_1;
    btree_1 -> group_symbol_table_1;
    group_symbol_table_1 -> symbol_table_entry_2;
    symbol_table_entry_2 -> object_header_1;
    object_header_1 -> continuation_message_1;
    continuation_message_1 -> symbol_table_message_3;
    continuation_message_1 -> attribute_message_1;
    symbol_table_message_3 -> btree_2;
    symbol_table_message_3 -> heap_2;
    btree_2 -> group_symbol_table_2;
    group_symbol_table_2 -> symbol_table_entry_3;
    symbol_table_entry_3 -> object_header_2;
    object_header_2 -> continuation_message_2;
    continuation_message_2 -> symbol_table_message_4;
    continuation_message_2 -> attribute_message_2;
    continuation_message_2 -> nil_message_1;
    symbol_table_message_4 -> btree_3;
    symbol_table_message_4 -> heap_3;
    btree_3 -> group_symbol_table_3;
}

# General TODOs

The superblock contains a "Base Address", which is the base address for all other addresses in the file.
We should test if we handle the case of superblocks starting at anything other than byte 0 correctly, and what the
base addresss is then.

# Things we ignore for now

- Fractal heaps
- Version 2 B-trees
- Shared Object Header Message Table

# Dataspaces

HDF5 dataspaces describe the shape of datasets in memory or in HDF5 files. Dataspaces can be empty (H5S_NULL), a singleton (H5S_SCALAR), or a multi-dimensional, regular grid (H5S_SIMPLE). Dataspaces can be re-shaped.

A scalar dataspace, H5S_SCALAR, represents just one element, a scalar. Note that the datatype of this one element may be very complex; example would be a compound structure with members being of any allowed HDF5 datatype, including multidimensional arrays, strings, and nested compound structures. By convention, the rank of a scalar dataspace is always 0 (zero); think of it geometrically as a single, dimensionless point, though that point may be complex.

A simple dataspace, H5S_SIMPLE , is a multidimensional array of elements. The dimensionality of the dataspace (or the rank of the array) is fixed and is defined at creation time. The size of each dimension can grow during the life time of the dataspace from the current size up to the maximum size. Both the current size and the maximum size are specified at creation time. The sizes of dimensions at any particular time in the life of a dataspace are called the current dimensions, or the dataspace extent. They can be queried along with the maximum sizes.
-}
-- TODO: add link name here

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
  | GraphContinuationMessage [GraphMessage]
  | GraphDataspaceMessage DataspaceMessageData
  | GraphDatatypeMessage DatatypeMessageData
  | GraphDataStorageFillValueMessage
  | GraphDataStorageFilterPipelineMessage DataStorageFilterPipelineMessageData
  | GraphDataStorageLayoutMessage DataStorageLayout
  | GraphObjectModificationTimeMessage Word32
  | GraphAttributeMessage BS.ByteString
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
        objectHeaderData <- BSL.hGet handle 2000
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
              forM
                (ohHeaderMessages header)
                (readGraphMessage depth putStrLnWithPrefix handle prefix)
            pure (GraphSymbolTableEntry (h5steObjectHeaderAddress e) linkName (GraphObjectHeader messages))

readGraphMessage :: Int -> (String -> IO ()) -> Handle -> [Char] -> Message -> IO GraphMessage
readGraphMessage depth putStrLnWithPrefix handle prefix message = do
  putStrLnWithPrefix ("message: " <> show message)
  case message of
    NilMessage -> pure GraphNilMessage
    DataspaceMessage d -> pure (GraphDataspaceMessage d)
    SymbolTableMessage btreeAddress heapAddress -> do
      putStrLnWithPrefix $ "seeking to " <> show btreeAddress <> "; check btree for symbol table"
      hSeek handle AbsoluteSeek (fromIntegral btreeAddress)
      blinkTreenode <- BSL.hGet handle 200
      case runGetOrFail getBLinkTreeNode blinkTreenode of
        Left _ -> error (prefix <> "error reading b-link node")
        Right (_, _, node@(BLinkTreeNodeGroup {})) -> do
          putStrLnWithPrefix ("tree node: " <> show node)

          putStrLnWithPrefix $ "seeking to heap at " <> show heapAddress
          hSeek handle AbsoluteSeek (fromIntegral heapAddress)
          heapHeaderData <- BSL.hGet handle 200
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
                    rawData <- BSL.hGet handle 200
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

              pure (GraphSymbolTableMessage btreeAddress (GraphTree treeEntries) (GraphHeap keysOnHeap))
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
          subMessages <- forM messages (readGraphMessage depth putStrLnWithPrefix handle prefix)
          pure (GraphContinuationMessage subMessages)
    DatatypeMessage d -> pure (GraphDatatypeMessage d)
    DataStorageFillValueMessage -> pure GraphDataStorageFillValueMessage
    DataStorageFilterPipelineMessage d -> pure (GraphDataStorageFilterPipelineMessage d)
    DataStorageLayoutMessage d -> pure (GraphDataStorageLayoutMessage d)
    ObjectModificationTimeMessage t -> pure (GraphObjectModificationTimeMessage t)
    AttributeMessage n -> pure (GraphAttributeMessage n)

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

readAndIncrement :: State Int Int
readAndIncrement = do
  current <- get
  put (current + 1)
  pure current

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
messageToDot origin (GraphContinuationMessage messages) = do
  thisNode <- addNode "continuation_message" Nothing
  addArrow origin thisNode
  mapM_ (messageToDot thisNode) messages
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
messageToDot origin (GraphAttributeMessage _attributeName) = do
  thisNode <- addNode "attribute_message" Nothing
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

main :: IO ()
main = do
  fileNameEncoded <- encodeUtf "/home/pmidden/178_data-00000.nx5"
  graph <- readH5ToGraph fileNameEncoded
  putStrLn (graphToDot graph)
