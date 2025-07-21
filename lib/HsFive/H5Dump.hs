{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module HsFive.H5Dump (h5dump) where

import Data.Int (Int64)
import Data.List (sortOn)
import qualified Data.List.NonEmpty as NE
import Data.Text (Text, intercalate, length, pack, replicate)
import HsFive.CoreTypes
  ( ByteOrder (LittleEndian),
    CharacterSet (CharacterSetAscii, CharacterSetUtf8),
    DataStorageLayout (LayoutContiguous, layoutContiguousSize),
    DataspaceDimension (DataspaceDimension, ddSize),
    Datatype (DatatypeFixedPoint, DatatypeFloatingPoint, DatatypeString, DatatypeVariableLengthString, fixedPointBitPrecision, fixedPointByteOrder, fixedPointSigned, floatingPointBitPrecision, floatingPointByteOrder),
    StringPadding (PaddingNull, PaddingNullTerminate, PaddingSpace),
  )
import HsFive.Types
  ( Attribute (Attribute, attributeData, attributeType),
    AttributeData (AttributeDataIntegral, AttributeDataString),
    DatasetData (DatasetData, datasetAttributes, datasetDatatype, datasetDimensions, datasetPath, datasetStorageLayout),
    GroupData (GroupData, groupAttributes, groupChildren, groupPath),
    Node (DatasetNode, GroupNode),
    attributeDimensions,
    attributeName,
    unwrapPath,
  )
import Prelude hiding (length, replicate)

newtype Indent = Indent Int

increaseIndent :: Indent -> Indent
increaseIndent (Indent n) = Indent (n + 3)

paddingToH5Dump :: StringPadding -> Text
paddingToH5Dump PaddingNullTerminate = "H5T_STR_NULLTERM"
paddingToH5Dump PaddingNull = "H5T_STR_NULLPAD"
paddingToH5Dump PaddingSpace = "H5T_STR_SPACEPAD"

charsetToH5Dump :: CharacterSet -> Text
charsetToH5Dump CharacterSetAscii = "H5T_CSET_ASCII"
charsetToH5Dump CharacterSetUtf8 = "H5T_CSET_UTF8"

stringSize :: [DataspaceDimension] -> Text -> Int64
stringSize dataspaceDimensions s = case dataspaceDimensions of
  [] -> fromIntegral (length s)
  [DataspaceDimension {ddSize}] -> fromIntegral (length s `div` fromIntegral ddSize)
  _ -> error ("got string with dataspace dimensions " <> show dataspaceDimensions)

showText :: (Show a) => a -> Text
showText = pack . show

dataspaceToH5Dump :: Indent -> [DataspaceDimension] -> [(Indent, Text)]
dataspaceToH5Dump m dataspaceDimensions =
  case dataspaceDimensions of
    [] -> [(m, "DATASPACE  SCALAR")]
    dims ->
      let inner = intercalate ", " (showText . ddSize <$> dims)
       in [(m, "DATASPACE  SIMPLE { ( " <> inner <> " ) / ( " <> inner <> " ) }")]

attributeDatatypeToH5Dump :: Indent -> Datatype -> [DataspaceDimension] -> Maybe Text -> [(Indent, Text)]
attributeDatatypeToH5Dump m (DatatypeFixedPoint {}) _ _ =
  [(m, "DATATYPE  H5T_STD_U16BE")]
attributeDatatypeToH5Dump m (DatatypeVariableLengthString padding charset _word) _ _ =
  [ (m, "DATATYPE  H5T_STRING {"),
    (increaseIndent m, "STRSIZE H5T_VARIABLE;"),
    (increaseIndent m, "STRPAD " <> paddingToH5Dump padding <> ";"),
    (increaseIndent m, "CSET " <> charsetToH5Dump charset <> ";"),
    (increaseIndent m, "CTYPE H5T_C_S1;"),
    (m, "}")
  ]
attributeDatatypeToH5Dump m (DatatypeString padding charset) dimensions (Just s) =
  [ (m, "DATATYPE  H5T_STRING {"),
    (increaseIndent m, "STRSIZE " <> showText (stringSize dimensions s) <> ";"),
    (increaseIndent m, "STRPAD " <> paddingToH5Dump padding <> ";"),
    (increaseIndent m, "CSET " <> charsetToH5Dump charset <> ";"),
    (increaseIndent m, "CTYPE H5T_C_S1;"),
    (m, "}")
  ]
attributeDatatypeToH5Dump _ e _ _ = error ("add this datatype: " <> show e)

attributeToH5Dump :: Indent -> Attribute -> [(Indent, Text)]
attributeToH5Dump n (Attribute {attributeName, attributeType, attributeDimensions, attributeData = AttributeDataString s}) =
  [ (increaseIndent n, "ATTRIBUTE \"" <> attributeName <> "\" {")
  ]
    <> attributeDatatypeToH5Dump (increaseIndent (increaseIndent n)) attributeType attributeDimensions (Just s)
    <> dataspaceToH5Dump (increaseIndent (increaseIndent n)) attributeDimensions
    <> [(increaseIndent n, "}")]
attributeToH5Dump n (Attribute {attributeName, attributeType, attributeDimensions, attributeData = AttributeDataIntegral integralValue}) =
  [ (increaseIndent n, "ATTRIBUTE \"" <> attributeName <> "\" {")
  ]
    <> attributeDatatypeToH5Dump (increaseIndent (increaseIndent n)) attributeType attributeDimensions Nothing
    <> dataspaceToH5Dump (increaseIndent (increaseIndent n)) attributeDimensions
    <> [(increaseIndent n, "DATA {")]
    <> [(increaseIndent n, ("(0):" <> pack (show integralValue)))]
    <> [(increaseIndent n, "}")]
    <> [(n, "}")]
attributeToH5Dump n (Attribute {attributeName, attributeType, attributeDimensions, attributeData = otherdata}) = error ("unnknown attribute type " <> show otherdata)

dumpGroup :: Indent -> GroupData -> [(Indent, Text)]
dumpGroup n (GroupData {groupPath, groupAttributes, groupChildren}) =
  let attributes = sortOn attributeName groupAttributes >>= attributeToH5Dump n
      subgroups = groupChildren >>= dumpNode (increaseIndent n)
   in [(n, "GROUP \"" <> unwrapPath groupPath "/" NE.last <> "\" {")] <> attributes <> subgroups <> [(n, "}")]

datatypeToH5Dump :: Indent -> Datatype -> DataStorageLayout -> [(Indent, Text)]
datatypeToH5Dump m (DatatypeVariableLengthString padding charset _word) _layout =
  [ (m, "DATATYPE  H5T_STRING {"),
    (increaseIndent m, "STRSIZE H5T_VARIABLE;"),
    (increaseIndent m, "STRPAD " <> paddingToH5Dump padding <> ";"),
    (increaseIndent m, "CSET " <> charsetToH5Dump charset <> ";"),
    (increaseIndent m, "CTYPE H5T_C_S1;"),
    (m, "}")
  ]
datatypeToH5Dump m (DatatypeString padding charset) (LayoutContiguous {layoutContiguousSize}) =
  [ (m, "DATATYPE  H5T_STRING {"),
    (increaseIndent m, "STRSIZE " <> showText layoutContiguousSize <> ";"),
    (increaseIndent m, "STRPAD " <> paddingToH5Dump padding <> ";"),
    (increaseIndent m, "CSET " <> charsetToH5Dump charset <> ";"),
    (increaseIndent m, "CTYPE H5T_C_S1;"),
    (m, "}")
  ]
datatypeToH5Dump m (DatatypeFloatingPoint {floatingPointByteOrder = LittleEndian, floatingPointBitPrecision = 64}) _layout = [(m, "DATATYPE  H5T_IEEE_F64LE")]
datatypeToH5Dump m (DatatypeFloatingPoint {floatingPointByteOrder = LittleEndian, floatingPointBitPrecision = 32}) _layout = [(m, "DATATYPE  H5T_IEEE_F32LE")]
datatypeToH5Dump m (DatatypeFixedPoint {fixedPointByteOrder = LittleEndian, fixedPointBitPrecision = 32, fixedPointSigned = True}) _layout = [(m, "DATATYPE  H5T_STD_I32LE")]
datatypeToH5Dump m (DatatypeFixedPoint {fixedPointByteOrder = LittleEndian, fixedPointBitPrecision = 64, fixedPointSigned = True}) _layout = [(m, "DATATYPE  H5T_STD_I64LE")]
datatypeToH5Dump m (DatatypeFixedPoint {fixedPointByteOrder = LittleEndian, fixedPointBitPrecision = 16, fixedPointSigned = False}) _layout = [(m, "DATATYPE  H5T_STD_U16LE")]
datatypeToH5Dump m type' _layout = [(m, "FIXME " <> showText type')]

dumpDataset :: Indent -> DatasetData -> [(Indent, Text)]
dumpDataset n (DatasetData {datasetPath, datasetDimensions, datasetDatatype, datasetStorageLayout, datasetAttributes}) =
  [(n, "DATASET \"" <> unwrapPath datasetPath "/" NE.last <> "\" {")]
    <> datatypeToH5Dump (increaseIndent n) datasetDatatype datasetStorageLayout
    <> dataspaceToH5Dump (increaseIndent n) datasetDimensions
    <> (sortOn attributeName datasetAttributes >>= attributeToH5Dump n)
    <> [(n, "}")]

dumpNode :: Indent -> Node -> [(Indent, Text)]
dumpNode n (GroupNode g) = dumpGroup n g
dumpNode n (DatasetNode g) = dumpDataset n g

h5dump :: GroupData -> Text
h5dump group =
  let showDump :: [(Indent, Text)] -> Text
      showDump lines' = intercalate "\n" ((\(Indent indent, line) -> replicate indent " " <> line) <$> lines')
   in showDump (dumpGroup (Indent 0) group)
