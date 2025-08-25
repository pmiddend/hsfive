{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module HsFive.H5Dump (h5dump) where

import qualified Data.ByteString.Lazy as BSL
import Data.List (sortOn)
import qualified Data.List.NonEmpty as NE
import Data.Text (Text, intercalate, length, pack, replicate)
import Data.Text.Encoding (decodeUtf8Lenient)
import HsFive.CoreTypes
  ( ByteOrder (BigEndian, LittleEndian),
    CharacterSet (CharacterSetAscii, CharacterSetUtf8),
    CompoundDatatypeMemberV1 (CompoundDatatypeMemberV1, cdm1Datatype, cdm1Name),
    CompoundDatatypeMemberV2 (CompoundDatatypeMemberV2, cdm2Datatype, cdm2Name),
    DataStorageLayout (LayoutContiguous, LayoutContiguousOld, layoutContiguousOldSizes, layoutContiguousSize),
    DataspaceDimension (ddSize),
    Datatype (DatatypeCompoundV1, DatatypeCompoundV2, DatatypeEnumeration, DatatypeFixedPoint, DatatypeFloatingPoint, DatatypeString, DatatypeVariableLengthString, fixedPointBitPrecision, fixedPointByteOrder, fixedPointSigned, floatingPointBitPrecision, floatingPointByteOrder),
    DatatypeMessageData (datatypeClass),
    Length,
    ReferenceType (ObjectReference),
    StringPadding (PaddingNull, PaddingNullTerminate, PaddingSpace),
    ddMaxSize,
  )
import HsFive.Types
  ( Attribute (Attribute, attributeData, attributeType),
    AttributeData (AttributeDataCompound, AttributeDataEnumeration, AttributeDataFloating, AttributeDataIntegral, AttributeDataReference, AttributeDataString, AttributeDataVariableLengthRaw),
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

showText :: (Show a) => a -> Text
showText = pack . show

nodeToDoc :: Node -> Doc ()
nodeToDoc (GroupNode g) = groupToDoc g
nodeToDoc (DatasetNode g) = datasetToDoc g
nodeToDoc (DatatypeNode g) = pretty (show g)

rightPad :: Text -> Int -> Text -> Text
rightPad c l t =
  let remainder = l - length t
   in if l < 0
        then t
        else t <> replicate remainder c

datatypeToDoc :: Datatype -> Maybe DataStorageLayout -> Bool -> Doc ann
datatypeToDoc (DatatypeVariableLengthString padding charset _word) _layout withPrefix =
  prefixAndBodyToDoc
    ((if withPrefix then "DATATYPE  " else mempty) <> "H5T_STRING")
    [ "STRSIZE H5T_VARIABLE;",
      "STRPAD " <> paddingToDoc padding <> ";",
      "CSET " <> charsetToDoc charset <> ";",
      "CTYPE H5T_C_S1;"
    ]
datatypeToDoc (DatatypeString padding charset _size) (Just (LayoutContiguous {layoutContiguousSize})) withPrefix =
  prefixAndBodyToDoc
    "DATATYPE  H5T_STRING"
    [ "STRSIZE " <> pretty (showText layoutContiguousSize) <> ";",
      "STRPAD " <> paddingToDoc padding <> ";",
      "CSET " <> charsetToDoc charset <> ";",
      "CTYPE H5T_C_S1;"
    ]
-- This [_, n] is a huge leap, but h5dump seems to do it this way?
datatypeToDoc (DatatypeString padding charset _size) (Just (LayoutContiguousOld {layoutContiguousOldSizes = [_, n]})) withPrefix =
  prefixAndBodyToDoc
    "DATATYPE  H5T_STRING"
    [ "STRSIZE " <> pretty (showText n) <> ";",
      "STRPAD " <> paddingToDoc padding <> ";",
      "CSET " <> charsetToDoc charset <> ";",
      "CTYPE H5T_C_S1;"
    ]
datatypeToDoc (DatatypeString padding charset size) _layout withPrefix =
  prefixAndBodyToDoc
    "DATATYPE  H5T_STRING"
    [ "STRSIZE " <> pretty size <> ";",
      "STRPAD " <> paddingToDoc padding <> ";",
      "CSET " <> charsetToDoc charset <> ";",
      "CTYPE H5T_C_S1;"
    ]
-- error ("string with size " <> show size <> ", unknown layout: " <> show _layout)
datatypeToDoc (DatatypeFloatingPoint {floatingPointByteOrder = LittleEndian, floatingPointBitPrecision = 64}) _layout withPrefix = (if withPrefix then "DATATYPE  " else mempty) <> "H5T_IEEE_F64LE"
datatypeToDoc (DatatypeFloatingPoint {floatingPointByteOrder = LittleEndian, floatingPointBitPrecision = 32}) _layout withPrefix = (if withPrefix then "DATATYPE  " else mempty) <> "H5T_IEEE_F32LE"
datatypeToDoc (DatatypeFloatingPoint {floatingPointByteOrder = BigEndian, floatingPointBitPrecision = 32}) _layout withPrefix = (if withPrefix then "DATATYPE  " else mempty) <> "H5T_IEEE_F32BE"
datatypeToDoc (DatatypeFixedPoint {fixedPointByteOrder = BigEndian, fixedPointBitPrecision = 16, fixedPointSigned = True}) _layout withPrefix = (if withPrefix then "DATATYPE  " else mempty) <> "H5T_STD_I16BE"
datatypeToDoc (DatatypeFixedPoint {fixedPointByteOrder = LittleEndian, fixedPointBitPrecision = 32, fixedPointSigned = True}) _layout withPrefix = (if withPrefix then "DATATYPE  " else mempty) <> "H5T_STD_I32LE"
datatypeToDoc (DatatypeFixedPoint {fixedPointByteOrder = BigEndian, fixedPointBitPrecision = 32, fixedPointSigned = True}) _layout withPrefix = (if withPrefix then "DATATYPE  " else mempty) <> "H5T_STD_I32BE"
datatypeToDoc (DatatypeFixedPoint {fixedPointByteOrder = LittleEndian, fixedPointBitPrecision = 64, fixedPointSigned = True}) _layout withPrefix = (if withPrefix then "DATATYPE  " else mempty) <> "H5T_STD_I64LE"
datatypeToDoc (DatatypeFixedPoint {fixedPointByteOrder = LittleEndian, fixedPointBitPrecision = 16, fixedPointSigned = False}) _layout withPrefix = (if withPrefix then "DATATYPE  " else mempty) <> "H5T_STD_U16LE"
datatypeToDoc (DatatypeEnumeration _baseType enumValues) _layout withPrefix =
  prefixAndBodyToDoc
    "DATATYPE  H5T_ENUM"
    ( [ "H5T_STD_I8LE;"
      ]
        <> ( ( \(enumName, enumValue) ->
                 pretty
                   ( rightPad
                       " "
                       19
                       ( "\""
                           <> decodeUtf8Lenient (BSL.toStrict enumName)
                           <> "\""
                       )
                   )
                   <> pretty enumValue
                   <> ";"
             )
               <$> enumValues
           )
    )
datatypeToDoc (DatatypeCompoundV2 members) _layout withPrefix =
  let compoundToDoc (CompoundDatatypeMemberV2 {cdm2Name, cdm2Datatype}) =
        datatypeToDoc (datatypeClass cdm2Datatype) Nothing False <> pretty (" \"" <> decodeUtf8Lenient cdm2Name <> "\";")
   in prefixAndBodyToDoc "DATATYPE  H5T_COMPOUND" (compoundToDoc <$> members)
-- datatypeToDoc dt _ = error ("invalid datatype: " <> show dt)
datatypeToDoc (DatatypeCompoundV1 members) _layout withPrefix =
  let compoundToDoc (CompoundDatatypeMemberV1 {cdm1Name, cdm1Datatype}) =
        datatypeToDoc (datatypeClass cdm1Datatype) Nothing False <> pretty (" \"" <> decodeUtf8Lenient cdm1Name <> "\";")
   in prefixAndBodyToDoc "DATATYPE  H5T_COMPOUND" (compoundToDoc <$> members)
datatypeToDoc dt _ _ = pretty ("invalid datatype: " <> show dt)

datasetToDoc :: DatasetData -> Doc ()
datasetToDoc (DatasetData {datasetPath, datasetDimensions, datasetDatatype, datasetStorageLayout, datasetAttributes}) =
  prefixAndBodyToDoc
    (namedPrefix "DATASET" (pretty (unwrapPath datasetPath "/" NE.last)))
    ( [ datatypeToDoc datasetDatatype (Just datasetStorageLayout) True,
        dataspaceToDoc datasetDimensions
      ]
        <> (attributeToDoc <$> sortOn attributeName datasetAttributes)
    )

prefixAndBodyToDoc :: Doc ann -> [Doc ann] -> Doc ann
prefixAndBodyToDoc prefix bodyValues = prefix <> " {" <> nest 3 (line <> vsep bodyValues) <> line <> "}"

paddingToDoc :: StringPadding -> Doc ann
paddingToDoc PaddingNullTerminate = "H5T_STR_NULLTERM"
paddingToDoc PaddingNull = "H5T_STR_NULLPAD"
paddingToDoc PaddingSpace = "H5T_STR_SPACEPAD"

charsetToDoc :: CharacterSet -> Doc ann
charsetToDoc CharacterSetAscii = "H5T_CSET_ASCII"
charsetToDoc CharacterSetUtf8 = "H5T_CSET_UTF8"

attributeDatatypeToDoc :: Datatype -> [DataspaceDimension] -> Maybe Text -> Doc ()
attributeDatatypeToDoc dt@(DatatypeFixedPoint {}) _ _ = datatypeToDoc dt Nothing True
attributeDatatypeToDoc dt@(DatatypeFloatingPoint {}) _ _ = datatypeToDoc dt Nothing True
-- attributeDatatypeToDoc (DatatypeFloatingPoint {floatingPointByteOrder = BigEndian, floatingPointBitPrecision = 32}) _ _ = "DATATYPE  H5T_STD_F32BE"
-- attributeDatatypeToDoc (DatatypeFloatingPoint {floatingPointByteOrder = LittleEndian, floatingPointBitPrecision = 64}) _ _ = "DATATYPE  H5T_STD_F64LE"
attributeDatatypeToDoc (DatatypeEnumeration baseType enumValues) _ _ = "DATATYPE  ENUM"
attributeDatatypeToDoc (DatatypeVariableLengthString padding charset _word) _ _ =
  prefixAndBodyToDoc
    "DATATYPE  H5T_STRING"
    [ "STRSIZE H5T_VARIABLE;",
      "STRPAD " <> paddingToDoc padding <> ";",
      "CSET " <> charsetToDoc charset <> ";",
      "CTYPE H5T_C_S1;"
    ]
attributeDatatypeToDoc (DatatypeString padding charset size) dimensions (Just s) =
  prefixAndBodyToDoc
    "DATATYPE  H5T_STRING"
    [ "STRSIZE " <> pretty size <> ";",
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
      let innerCurrentDims = pretty (intercalate ", " (showText . ddSize <$> dims))
          prettyOptionalDim :: Maybe Length -> Text
          prettyOptionalDim Nothing = "H5S_UNLIMITED"
          prettyOptionalDim (Just d) = showText d
          innerMaxDims = pretty (intercalate ", " (prettyOptionalDim . ddMaxSize <$> dims))
       in "DATASPACE  SIMPLE { ( " <> innerCurrentDims <> " ) / ( " <> innerMaxDims <> " ) }"

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
      dataspaceToDoc attributeDimensions
      -- prefixAndBodyToDoc "DATA" ["(0): " <> pretty integralValue]
    ]
attributeToDoc (Attribute {attributeName, attributeType, attributeDimensions, attributeData = AttributeDataFloating floatingValue}) =
  prefixAndBodyToDoc
    (namedPrefix "ATTRIBUTE" (pretty attributeName))
    [ attributeDatatypeToDoc attributeType attributeDimensions Nothing,
      dataspaceToDoc attributeDimensions
      -- prefixAndBodyToDoc "DATA" ["(0): " <> pretty floatingValue]
    ]
attributeToDoc (Attribute {attributeName, attributeType, attributeDimensions, attributeData = AttributeDataEnumeration enumMap enumValue}) =
  prefixAndBodyToDoc
    (namedPrefix "ATTRIBUTE" (pretty attributeName))
    [ attributeDatatypeToDoc attributeType attributeDimensions Nothing,
      dataspaceToDoc attributeDimensions
      -- prefixAndBodyToDoc "DATA" ["(0): " <> pretty enumValue]
    ]
attributeToDoc (Attribute {attributeData = AttributeDataReference ObjectReference _}) = "DATATYPE  H5T_REFERENCE { H5T_STD_REF_OBJECT }"
attributeToDoc (Attribute {attributeName, attributeType, attributeDimensions, attributeData = AttributeDataCompound members}) = "DATATYPE COMPOUND TODO"
attributeToDoc (Attribute {attributeName, attributeType, attributeDimensions, attributeData = AttributeDataVariableLengthRaw rawBytes}) = "DATATYPE VARIABLE LENGTH TODO"
attributeToDoc (Attribute {attributeName, attributeType, attributeDimensions, attributeData}) = error ("invalid attribute type: " <> show attributeData)

groupToDoc :: GroupData -> Doc ()
groupToDoc (GroupData {groupPath, groupAttributes, groupChildren}) =
  prefixAndBodyToDoc
    (namedPrefix "GROUP" (pretty (unwrapPath groupPath "/" NE.last)))
    ((attributeToDoc <$> sortOn attributeName groupAttributes) <> (nodeToDoc <$> groupChildren))

h5dump :: FilePath -> GroupData -> Text
h5dump fileName g = renderStrict $ layoutPretty defaultLayoutOptions $ ("HDF5 \"" <> pretty fileName <> "\" {" <> line <> groupToDoc g <> line <> "}")
