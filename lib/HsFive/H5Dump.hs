{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module HsFive.H5Dump (h5dump) where

import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int64)
import Data.List (sortOn)
import qualified Data.List.NonEmpty as NE
import Data.Text (Text, intercalate, length, pack)
import Data.Text.Encoding (decodeUtf8Lenient)
import HsFive.CoreTypes
  ( ByteOrder (BigEndian, LittleEndian),
    CharacterSet (CharacterSetAscii, CharacterSetUtf8),
    CompoundDatatypeMemberV2 (CompoundDatatypeMemberV2, cdm2Datatype, cdm2Name),
    DataStorageLayout (LayoutContiguous, LayoutContiguousOld, layoutContiguousOldSizes, layoutContiguousSize),
    DataspaceDimension (DataspaceDimension, ddSize),
    Datatype (DatatypeCompoundV2, DatatypeEnumeration, DatatypeFixedPoint, DatatypeFloatingPoint, DatatypeString, DatatypeVariableLengthString, fixedPointBitPrecision, fixedPointByteOrder, fixedPointSigned, floatingPointBitPrecision, floatingPointByteOrder),
    DatatypeMessageData (datatypeClass),
    ReferenceType (ObjectReference),
    StringPadding (PaddingNull, PaddingNullTerminate, PaddingSpace),
  )
import HsFive.Types
  ( Attribute (Attribute, attributeData, attributeType),
    AttributeData (AttributeDataFloating, AttributeDataIntegral, AttributeDataReference, AttributeDataString),
    DatasetData (DatasetData, datasetAttributes, datasetDatatype, datasetDimensions, datasetPath, datasetStorageLayout),
    GroupData (GroupData, groupAttributes, groupChildren, groupPath),
    Node (DatasetNode, DatatypeNode, GroupNode),
    attributeDimensions,
    attributeName,
    unwrapPath,
  )
import Prettyprinter (Doc, Pretty (pretty), defaultLayoutOptions, layoutPretty, line, nest, vsep)
import Prettyprinter.Render.Text (renderStrict)
import Prelude hiding (length, replicate)

stringSize :: [DataspaceDimension] -> Text -> Int64
stringSize dataspaceDimensions s = case dataspaceDimensions of
  [] -> fromIntegral (length s)
  [DataspaceDimension {ddSize}] -> fromIntegral (length s `div` fromIntegral ddSize)
  _ -> error ("got string with dataspace dimensions " <> show dataspaceDimensions)

showText :: (Show a) => a -> Text
showText = pack . show

nodeToDoc :: Node -> Doc ()
nodeToDoc (GroupNode g) = groupToDoc g
nodeToDoc (DatasetNode g) = datasetToDoc g
nodeToDoc (DatatypeNode g) = pretty (show g)

datatypeToDoc :: Datatype -> Maybe DataStorageLayout -> Doc ann
datatypeToDoc (DatatypeVariableLengthString padding charset _word) _layout =
  prefixAndBodyToDoc
    "DATATYPE  H5T_STRING"
    [ "STRSIZE H5T_VARIABLE;",
      "STRPAD " <> paddingToDoc padding <> ";",
      "CSET " <> charsetToDoc charset <> ";",
      "CTYPE H5T_C_S1;"
    ]
datatypeToDoc (DatatypeString padding charset _size) (Just (LayoutContiguous {layoutContiguousSize})) =
  prefixAndBodyToDoc
    "DATATYPE  H5T_STRING"
    [ "STRSIZE " <> pretty (showText layoutContiguousSize) <> ";",
      "STRPAD " <> paddingToDoc padding <> ";",
      "CSET " <> charsetToDoc charset <> ";",
      "CTYPE H5T_C_S1;"
    ]
-- This [_, n] is a huge leap, but h5dump seems to do it this way?
datatypeToDoc (DatatypeString padding charset _size) (Just (LayoutContiguousOld {layoutContiguousOldSizes = [_, n]})) =
  prefixAndBodyToDoc
    "DATATYPE  H5T_STRING"
    [ "STRSIZE " <> pretty (showText n) <> ";",
      "STRPAD " <> paddingToDoc padding <> ";",
      "CSET " <> charsetToDoc charset <> ";",
      "CTYPE H5T_C_S1;"
    ]
datatypeToDoc (DatatypeString padding charset size) _layout =
  prefixAndBodyToDoc
    "DATATYPE  H5T_STRING"
    [ "STRSIZE " <> pretty size <> ";",
      "STRPAD " <> paddingToDoc padding <> ";",
      "CSET " <> charsetToDoc charset <> ";",
      "CTYPE H5T_C_S1;"
    ]
-- error ("string with size " <> show size <> ", unknown layout: " <> show _layout)
datatypeToDoc (DatatypeFloatingPoint {floatingPointByteOrder = LittleEndian, floatingPointBitPrecision = 64}) _layout = "DATATYPE  H5T_IEEE_F64LE"
datatypeToDoc (DatatypeFloatingPoint {floatingPointByteOrder = LittleEndian, floatingPointBitPrecision = 32}) _layout = "DATATYPE  H5T_IEEE_F32LE"
datatypeToDoc (DatatypeFloatingPoint {floatingPointByteOrder = BigEndian, floatingPointBitPrecision = 32}) _layout = "DATATYPE  H5T_IEEE_F32BE"
datatypeToDoc (DatatypeFixedPoint {fixedPointByteOrder = BigEndian, fixedPointBitPrecision = 16, fixedPointSigned = True}) _layout = "DATATYPE  H5T_STD_I16BE"
datatypeToDoc (DatatypeFixedPoint {fixedPointByteOrder = LittleEndian, fixedPointBitPrecision = 32, fixedPointSigned = True}) _layout = "DATATYPE  H5T_STD_I32LE"
datatypeToDoc (DatatypeFixedPoint {fixedPointByteOrder = BigEndian, fixedPointBitPrecision = 32, fixedPointSigned = True}) _layout = "DATATYPE  H5T_STD_I32BE"
datatypeToDoc (DatatypeFixedPoint {fixedPointByteOrder = LittleEndian, fixedPointBitPrecision = 64, fixedPointSigned = True}) _layout = "DATATYPE  H5T_STD_I64LE"
datatypeToDoc (DatatypeFixedPoint {fixedPointByteOrder = LittleEndian, fixedPointBitPrecision = 16, fixedPointSigned = False}) _layout = "DATATYPE  H5T_STD_U16LE"
datatypeToDoc (DatatypeEnumeration enumValues) _layout =
  prefixAndBodyToDoc
    "DATATYPE  H5T_ENUM {"
    ( [ "H5T_STD_I8LE;"
      ]
        <> ( ( \(enumName, enumValue) ->
                 "\""
                   <> pretty (decodeUtf8Lenient (BSL.toStrict enumName))
                   <> "\"            "
                   <> pretty enumValue
             )
               <$> enumValues
           )
    )
datatypeToDoc (DatatypeCompoundV2 members) _layout =
  let compoundToDoc (CompoundDatatypeMemberV2 {cdm2Name, cdm2Datatype}) =
        datatypeToDoc (datatypeClass cdm2Datatype) Nothing <> pretty ("\"" <> decodeUtf8Lenient cdm2Name <> "\"")
   in prefixAndBodyToDoc "DATATYPE  H5T_COMPOUND {" (compoundToDoc <$> members)
-- datatypeToDoc dt _ = error ("invalid datatype: " <> show dt)
datatypeToDoc dt _ = pretty ("invalid datatype: " <> show dt)

datasetToDoc :: DatasetData -> Doc ()
datasetToDoc (DatasetData {datasetPath, datasetDimensions, datasetDatatype, datasetStorageLayout, datasetAttributes}) =
  prefixAndBodyToDoc
    (namedPrefix "DATASET" (pretty (unwrapPath datasetPath "/" NE.last)))
    ( [ datatypeToDoc datasetDatatype (Just datasetStorageLayout),
        dataspaceToDoc datasetDimensions
      ]
        <> (attributeToDoc <$> sortOn attributeName datasetAttributes)
    )

prefixAndBodyToDoc :: Doc ann -> [Doc ann] -> Doc ann
prefixAndBodyToDoc prefix bodyValues = prefix <> " {" <> nest 2 (line <> vsep bodyValues) <> line <> "}"

paddingToDoc :: StringPadding -> Doc ann
paddingToDoc PaddingNullTerminate = "H5T_STR_NULLTERM"
paddingToDoc PaddingNull = "H5T_STR_NULLPAD"
paddingToDoc PaddingSpace = "H5T_STR_SPACEPAD"

charsetToDoc :: CharacterSet -> Doc ann
charsetToDoc CharacterSetAscii = "H5T_CSET_ASCII"
charsetToDoc CharacterSetUtf8 = "H5T_CSET_UTF8"

attributeDatatypeToDoc :: Datatype -> [DataspaceDimension] -> Maybe Text -> Doc ()
attributeDatatypeToDoc (DatatypeFixedPoint {}) _ _ = "DATATYPE  H5T_STD_U16BE"
attributeDatatypeToDoc (DatatypeFloatingPoint {floatingPointByteOrder = BigEndian, floatingPointBitPrecision = 32}) _ _ = "DATATYPE  H5T_STD_F32BE"
attributeDatatypeToDoc (DatatypeVariableLengthString padding charset _word) _ _ =
  prefixAndBodyToDoc
    "DATATYPE  H5T_STRING"
    [ "STRSIZE H5T_VARIABLE;",
      "STRPAD " <> paddingToDoc padding <> ";",
      "CSET " <> charsetToDoc charset <> ";",
      "CTYPE H5T_C_S1;"
    ]
attributeDatatypeToDoc (DatatypeString padding charset _size) dimensions (Just s) =
  prefixAndBodyToDoc
    "DATATYPE  H5T_STRING"
    [ "STRSIZE " <> pretty (stringSize dimensions s) <> ";",
      "STRPAD " <> paddingToDoc padding <> ";",
      "CSET " <> charsetToDoc charset <> ";",
      "CTYPE H5T_C_S1;"
    ]
attributeDatatypeToDoc e _ _ = error ("add this datatype: " <> show e)

dataspaceToDoc :: [DataspaceDimension] -> Doc ann
dataspaceToDoc dataspaceDimensions =
  case dataspaceDimensions of
    [] -> "DATASPACE  SCALAR"
    dims ->
      let inner = pretty (intercalate ", " (showText . ddSize <$> dims))
       in "DATASPACE  SIMPLE { ( " <> inner <> " ) / ( " <> inner <> " ) }"

namedPrefix :: Doc ann -> Doc ann -> Doc ann
namedPrefix name value = name <> " \"" <> value <> "\""

attributeToDoc :: Attribute -> Doc ()
attributeToDoc (Attribute {attributeName, attributeType, attributeDimensions, attributeData = AttributeDataString s}) =
  prefixAndBodyToDoc
    (namedPrefix "ATTRIBUTE" (pretty attributeName))
    [ attributeDatatypeToDoc attributeType attributeDimensions (Just s),
      dataspaceToDoc attributeDimensions
    ]
attributeToDoc (Attribute {attributeName, attributeType, attributeDimensions, attributeData = AttributeDataIntegral integralValue}) =
  prefixAndBodyToDoc
    (namedPrefix "ATTRIBUTE" (pretty attributeName))
    [ attributeDatatypeToDoc attributeType attributeDimensions Nothing,
      dataspaceToDoc attributeDimensions,
      prefixAndBodyToDoc "DATA" ["(0): " <> pretty integralValue]
    ]
attributeToDoc (Attribute {attributeName, attributeType, attributeDimensions, attributeData = AttributeDataFloating floatingValue}) =
  prefixAndBodyToDoc
    (namedPrefix "ATTRIBUTE" (pretty attributeName))
    [ attributeDatatypeToDoc attributeType attributeDimensions Nothing,
      dataspaceToDoc attributeDimensions,
      prefixAndBodyToDoc "DATA" ["(0): " <> pretty floatingValue]
    ]
attributeToDoc (Attribute {attributeData = AttributeDataReference ObjectReference _}) = "DATATYPE  H5T_REFERENCE { H5T_STD_REF_OBJECT }"
attributeToDoc (Attribute {attributeName, attributeType, attributeDimensions, attributeData}) = error ("invalid attribute type: " <> show attributeData)

groupToDoc :: GroupData -> Doc ()
groupToDoc (GroupData {groupPath, groupAttributes, groupChildren}) =
  prefixAndBodyToDoc
    (namedPrefix "GROUP" (pretty (unwrapPath groupPath "/" NE.last)))
    ((attributeToDoc <$> sortOn attributeName groupAttributes) <> (nodeToDoc <$> groupChildren))

h5dump :: GroupData -> Text
h5dump g = renderStrict $ layoutPretty defaultLayoutOptions $ groupToDoc g
