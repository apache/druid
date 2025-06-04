/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.druid.segment.virtual;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.expression.NestedDataExpressions;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.column.ValueTypes;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.nested.CompressedNestedDataComplexColumn;
import org.apache.druid.segment.nested.NestedCommonFormatColumn;
import org.apache.druid.segment.nested.NestedDataComplexColumn;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.nested.NestedFieldDictionaryEncodedColumn;
import org.apache.druid.segment.nested.NestedPathArrayElement;
import org.apache.druid.segment.nested.NestedPathFinder;
import org.apache.druid.segment.nested.NestedPathPart;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.nested.VariantColumn;
import org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier;
import org.apache.druid.segment.vector.BaseDoubleVectorValueSelector;
import org.apache.druid.segment.vector.BaseFloatVectorValueSelector;
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Optimized virtual column that can make direct selectors into a {@link NestedDataComplexColumn} or any associated
 * nested fields ({@link NestedFieldDictionaryEncodedColumn}) including using
 * their indexes.
 * <p>
 * This virtual column is used for the SQL operators JSON_VALUE (if {@link #isProcessFromRaw()} is set to false) or
 * JSON_QUERY (if it is true), and accepts 'JSONPath' or 'jq' syntax string representations of paths, or a parsed
 * list of {@link NestedPathPart} in order to determine what should be selected from the column.
 * <p>
 * Type information for nested fields is completely absent in the SQL planner, so it guesses the best it can to set
 * {@link #getExpectedType()} from the context of how something is being used, e.g. an aggregators default type or an
 * explicit cast, or, if using the 'RETURNING' syntax which explicitly specifies type. This might not be the same as
 * if it had actual type information, but, we try to stick with whatever we chose there to do the best we can for now.
 * <p>
 * Since {@link #capabilities(ColumnInspector, String)} is determined by the {@link #getExpectedType()}, the results
 * will be best effor cast to the expected type if the column is not natively the expected type so that this column can
 * fulfill the contract of the type of selector that is likely to be created to read this column.
 */
public class NestedFieldVirtualColumn implements VirtualColumn
{
  private static final NestedDataExpressions.JsonQueryExprMacro JSON_QUERY = new NestedDataExpressions.JsonQueryExprMacro();
  private static final NestedDataExpressions.JsonValueExprMacro JSON_VALUE = new NestedDataExpressions.JsonValueExprMacro();

  private final String outputName;
  private final NestedFieldSpec fieldSpec;

  private final boolean hasNegativeArrayIndex;

  @JsonCreator
  public NestedFieldVirtualColumn(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("outputName") String outputName,
      @JsonProperty("expectedType") @Nullable ColumnType expectedType,
      @JsonProperty("pathParts") @Nullable List<NestedPathPart> parts,
      @JsonProperty("processFromRaw") @Nullable Boolean processFromRaw,
      @JsonProperty("path") @Nullable String path,
      @JsonProperty("useJqSyntax") @Nullable Boolean useJqSyntax
  )
  {
    this.outputName = outputName;
    if (path != null) {
      Preconditions.checkArgument(parts == null, "Cannot define both 'path' and 'pathParts'");
    } else if (parts == null) {
      throw new IllegalArgumentException("Must define exactly one of 'path' or 'pathParts'");
    }
    final List<NestedPathPart> pathParts;
    if (parts != null) {
      pathParts = parts;
    } else {
      boolean isInputJq = useJqSyntax != null && useJqSyntax;
      pathParts = isInputJq ? NestedPathFinder.parseJqPath(path) : NestedPathFinder.parseJsonPath(path);
    }
    boolean hasNegative = false;
    for (NestedPathPart part : pathParts) {
      if (part instanceof NestedPathArrayElement) {
        NestedPathArrayElement elementPart = (NestedPathArrayElement) part;
        if (elementPart.getIndex() < 0) {
          hasNegative = true;
          break;
        }
      }
    }
    this.hasNegativeArrayIndex = hasNegative;
    this.fieldSpec = new NestedFieldSpec(
        columnName,
        expectedType,
        pathParts,
        processFromRaw != null && processFromRaw
    );
  }

  @VisibleForTesting
  public NestedFieldVirtualColumn(
      String columnName,
      String path,
      String outputName
  )
  {
    this(columnName, outputName, null, null, null, path, false);
  }

  @VisibleForTesting
  public NestedFieldVirtualColumn(
      String columnName,
      String path,
      String outputName,
      @Nullable ColumnType expectedType
  )
  {
    this(columnName, outputName, expectedType, null, null, path, false);
  }

  @Nullable
  @Override
  public byte[] getCacheKey()
  {
    final String partsString = NestedPathFinder.toNormalizedJsonPath(fieldSpec.parts);
    return new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_USER_DEFINED).appendString("nested-field")
                                                                                   .appendString(outputName)
                                                                                   .appendString(fieldSpec.columnName)
                                                                                   .appendString(partsString)
                                                                                   .appendBoolean(fieldSpec.processFromRaw)
                                                                                   .build();
  }

  @JsonProperty
  @Override
  public String getOutputName()
  {
    return outputName;
  }

  @JsonProperty
  public String getColumnName()
  {
    return fieldSpec.columnName;
  }

  @JsonProperty("pathParts")
  public List<NestedPathPart> getPathParts()
  {
    return fieldSpec.parts;
  }

  @Nullable
  @JsonProperty
  public ColumnType getExpectedType()
  {
    return fieldSpec.expectedType;
  }

  @JsonProperty
  public boolean isProcessFromRaw()
  {
    return fieldSpec.processFromRaw;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory factory
  )
  {
    // this dimension selector is used for realtime queries, nested paths are not themselves dictionary encoded until
    // written to segment, so we fall back to processing the structured data from a column value selector on the
    // complex column
    ColumnValueSelector<?> valueSelector = makeColumnValueSelector(dimensionSpec.getOutputName(), factory);
    return dimensionSpec.decorate(new FieldDimensionSelector(valueSelector));
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory factory
  )
  {
    // realtime selectors have no optimization, fallback to json_query/json_value expressions
    final Expr identifier = Parser.identifier(fieldSpec.columnName);
    final Expr path = Parser.constant(NestedPathFinder.toNormalizedJsonPath(fieldSpec.parts));
    final Expr jsonExpr;
    if (fieldSpec.processFromRaw) {
      // processFromRaw is true that means JSON_QUERY, which can return partial object results
      jsonExpr = JSON_QUERY.apply(List.of(identifier, path));
    } else {
      // otherwise, this virtual column is JSON_VALUE which only returns primitives
      final List<Expr> args;
      if (fieldSpec.expectedType != null) {
        final Expr castType = Parser.constant(
            ExpressionType.fromColumnTypeStrict(fieldSpec.expectedType).asTypeString()
        );
        args = List.of(identifier, path, castType);
      } else {
        args = List.of(identifier, path);
      }
      jsonExpr = JSON_VALUE.apply(args);
    }
    return ExpressionSelectors.makeColumnValueSelector(factory, jsonExpr);
  }

  @Nullable
  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    ColumnHolder holder = columnSelector.getColumnHolder(fieldSpec.columnName);
    if (holder == null) {
      // column doesn't exist
      return dimensionSpec.decorate(DimensionSelector.constant(null, dimensionSpec.getExtractionFn()));
    }
    if (hasNegativeArrayIndex) {
      // negative array elements in a path expression mean that values should be fetched 'from the end' of the array
      // if the path has negative array elements, then we have to use the 'raw' processing of the FieldDimensionSelector
      // created with the column selector factory instead of using the optimized nested field column, return null
      // to fall through
      return null;
    }

    return dimensionSpec.decorate(makeDimensionSelectorUndecorated(holder, offset, dimensionSpec.getExtractionFn()));
  }

  private DimensionSelector makeDimensionSelectorUndecorated(
      ColumnHolder holder,
      ReadableOffset offset,
      @Nullable ExtractionFn extractionFn
  )
  {
    BaseColumn theColumn = holder.getColumn();
    if (theColumn instanceof NestedDataComplexColumn) {
      final NestedDataComplexColumn column = (NestedDataComplexColumn) theColumn;
      return column.makeDimensionSelector(fieldSpec.parts, offset, extractionFn);
    }

    // not a nested column, but we can still do stuff if the path is the 'root', indicated by an empty path parts
    if (fieldSpec.parts.isEmpty()) {
      // dictionary encoded columns do not typically implement the value selector methods (getLong, getDouble, getFloat)
      // nothing *should* be using a dimension selector to call the numeric getters, but just in case... wrap their
      // selector in a "best effort" casting selector to implement them
      if (theColumn instanceof DictionaryEncodedColumn) {
        final DictionaryEncodedColumn<?> column = (DictionaryEncodedColumn<?>) theColumn;
        return new BestEffortCastingValueSelector(column.makeDimensionSelector(offset, extractionFn));
      }
      // for non-dictionary encoded columns, wrap a value selector to make it appear as a dimension selector
      return ValueTypes.makeNumericWrappingDimensionSelector(
          holder.getCapabilities().getType(),
          theColumn.makeColumnValueSelector(offset),
          extractionFn
      );
    }

    if (isRootArrayElementPathAndArrayColumn(theColumn)) {
      final VariantColumn<?> arrayColumn = (VariantColumn<?>) theColumn;
      ColumnValueSelector<?> arraySelector = arrayColumn.makeColumnValueSelector(offset);
      final int elementNumber = ((NestedPathArrayElement) fieldSpec.parts.get(0)).getIndex();
      if (elementNumber < 0) {
        throw new IAE("Cannot make array element selector, negative array index not supported");
      }
      return new BaseSingleValueDimensionSelector()
      {
        @Nullable
        @Override
        protected String getValue()
        {
          Object o = arraySelector.getObject();
          if (o instanceof Object[]) {
            Object[] array = (Object[]) o;
            if (elementNumber < array.length) {
              Object element = array[elementNumber];
              if (element == null) {
                return null;
              }
              return String.valueOf(element);
            }
          }
          return null;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          arraySelector.inspectRuntimeShape(inspector);
        }
      };
    }

    // we are not a nested column and are being asked for a path that will never exist, so we are nil selector
    return DimensionSelector.constant(null, extractionFn);
  }


  @Nullable
  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    ColumnHolder holder = columnSelector.getColumnHolder(fieldSpec.columnName);
    if (holder == null) {
      return NilColumnValueSelector.instance();
    }
    BaseColumn theColumn = holder.getColumn();

    if (fieldSpec.processFromRaw || hasNegativeArrayIndex) {
      // if the path has negative array elements, or has set the flag to process 'raw' values explicitly (JSON_QUERY),
      // then we use the 'raw' processing of the RawFieldColumnSelector/RawFieldLiteralColumnValueSelector created
      // with the column selector factory instead of using the optimized nested field column
      return null;
    }

    // "JSON_VALUE", which only returns literals, on a NestedDataComplexColumn, so we can use the fields value selector

    if (theColumn instanceof NestedDataComplexColumn) {
      final NestedDataComplexColumn column = (NestedDataComplexColumn) theColumn;
      final ColumnType fieldType = column.getFieldLogicalType(fieldSpec.parts);
      if (fieldType != null && fieldSpec.expectedType != null && !fieldSpec.expectedType.equals(fieldType)) {
        return ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            column.makeColumnValueSelector(fieldSpec.parts, offset),
            fieldType,
            fieldSpec.expectedType
        );
      }
      return column.makeColumnValueSelector(fieldSpec.parts, offset);
    }

    // not a nested column, but we can still do stuff if the path is the 'root', indicated by an empty path parts
    if (fieldSpec.parts.isEmpty()) {
      // dictionary encoded columns do not typically implement the value selector methods (getLong, getDouble, getFloat)
      // so we want to wrap their selector in a "best effort" casting selector to implement them
      if (theColumn instanceof DictionaryEncodedColumn && !(theColumn instanceof VariantColumn)) {
        final DictionaryEncodedColumn<?> column = (DictionaryEncodedColumn<?>) theColumn;
        return new BestEffortCastingValueSelector(column.makeDimensionSelector(offset, null));
      }
      // otherwise it is probably cool to pass through the value selector directly, if numbers make sense the selector
      // very likely implemented them, and everyone implements getObject if not
      return theColumn.makeColumnValueSelector(offset);
    }

    if (isRootArrayElementPathAndArrayColumn(theColumn)) {
      final VariantColumn<?> arrayColumn = (VariantColumn<?>) theColumn;
      ColumnValueSelector<?> arraySelector = arrayColumn.makeColumnValueSelector(offset);
      final int elementNumber = ((NestedPathArrayElement) fieldSpec.parts.get(0)).getIndex();
      if (elementNumber < 0) {
        throw new IAE("Cannot make array element selector, negative array index not supported");
      }
      return new ArrayElementColumnValueSelector(arraySelector, elementNumber);
    }

    // we are not a nested column and are being asked for a path that will never exist, so we are nil selector
    return NilColumnValueSelector.instance();
  }

  @Override
  public boolean canVectorize(ColumnInspector inspector)
  {
    return !hasNegativeArrayIndex;
  }

  @Nullable
  @Override
  public SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    ColumnHolder holder = columnSelector.getColumnHolder(fieldSpec.columnName);
    if (holder == null) {
      return dimensionSpec.decorate(NilVectorSelector.create(offset));
    }

    return dimensionSpec.decorate(makeSingleValueVectorDimensionSelectorUndecorated(holder, offset));
  }

  private SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelectorUndecorated(
      ColumnHolder holder,
      ReadableVectorOffset offset
  )
  {
    BaseColumn theColumn = holder.getColumn();
    if (theColumn instanceof NestedDataComplexColumn) {
      final NestedDataComplexColumn column = (NestedDataComplexColumn) theColumn;
      return column.makeSingleValueDimensionVectorSelector(fieldSpec.parts, offset);
    }

    // not a nested column, but we can still do stuff if the path is the 'root', indicated by an empty path parts
    if (fieldSpec.parts.isEmpty()) {
      // we will not end up here unless underlying column capabilities lied about something being dictionary encoded...
      // so no need for magic casting like nonvectorized engine
      return ((DictionaryEncodedColumn) theColumn).makeSingleValueDimensionVectorSelector(offset);
    }

    // we are not a nested column and are being asked for a path that will never exist, so we are nil selector
    return NilVectorSelector.create(offset);
  }


  @Nullable
  @Override
  public VectorObjectSelector makeVectorObjectSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    ColumnHolder holder = columnSelector.getColumnHolder(fieldSpec.columnName);
    if (holder == null) {
      return NilVectorSelector.create(offset);
    }
    BaseColumn column = holder.getColumn();


    if (column instanceof NestedDataComplexColumn) {
      final NestedDataComplexColumn complexColumn = (NestedDataComplexColumn) column;
      if (fieldSpec.processFromRaw) {
        // processFromRaw is true, that means JSON_QUERY, which can return partial results, otherwise this virtual column
        // is JSON_VALUE which only returns literals, so we can use the nested columns value selector
        return new RawFieldVectorObjectSelector(complexColumn.makeVectorObjectSelector(offset), fieldSpec.parts);
      }
      final ColumnType leastRestrictiveType = complexColumn.getFieldLogicalType(fieldSpec.parts);
      if (leastRestrictiveType != null && leastRestrictiveType.isNumeric() && !Types.isNumeric(fieldSpec.expectedType)) {
        return ExpressionVectorSelectors.castValueSelectorToObject(
            offset,
            columnName,
            complexColumn.makeVectorValueSelector(fieldSpec.parts, offset),
            leastRestrictiveType,
            fieldSpec.expectedType == null ? ColumnType.STRING : fieldSpec.expectedType
        );
      }
      final VectorObjectSelector objectSelector = complexColumn.makeVectorObjectSelector(fieldSpec.parts, offset);

      return castVectorObjectSelectorIfNeeded(columnName, offset, leastRestrictiveType, objectSelector);
    }
    // not a nested column, but we can still do stuff if the path is the 'root', indicated by an empty path parts
    if (fieldSpec.parts.isEmpty()) {
      ColumnCapabilities capabilities = holder.getCapabilities();
      // expectedType shouldn't possibly be null if we are being asked for an object selector and the underlying column
      // is numeric, else we would have been asked for a value selector
      Preconditions.checkArgument(
          fieldSpec.expectedType != null,
          "Asked for a VectorObjectSelector on a numeric column, 'expectedType' must not be null"
      );
      if (capabilities.isNumeric()) {
        return ExpressionVectorSelectors.castValueSelectorToObject(
            offset,
            fieldSpec.columnName,
            column.makeVectorValueSelector(offset),
            capabilities.toColumnType(),
            fieldSpec.expectedType
        );
      }
      final VectorObjectSelector delegate = column.makeVectorObjectSelector(offset);
      return castVectorObjectSelectorIfNeeded(columnName, offset, capabilities.toColumnType(), delegate);
    }

    if (isRootArrayElementPathAndArrayColumn(column)) {
      final VariantColumn<?> arrayColumn = (VariantColumn<?>) column;
      final ExpressionType elementType = ExpressionType.fromColumnTypeStrict(
          arrayColumn.getLogicalType().isArray() ? arrayColumn.getLogicalType().getElementType() : arrayColumn.getLogicalType()
      );
      final ExpressionType castTo = fieldSpec.expectedType == null
                                    ? ExpressionType.STRING
                                    : ExpressionType.fromColumnTypeStrict(fieldSpec.expectedType);
      VectorObjectSelector arraySelector = arrayColumn.makeVectorObjectSelector(offset);
      final int elementNumber = ((NestedPathArrayElement) fieldSpec.parts.get(0)).getIndex();
      if (elementNumber < 0) {
        throw new IAE("Cannot make array element selector, negative array index not supported");
      }
      return new ArrayElementVectorObjectSelector(arraySelector, offset, elementNumber, elementType, castTo);
    }

    // we are not a nested column and are being asked for a path that will never exist, so we are nil selector
    return NilVectorSelector.create(offset);
  }


  @Nullable
  @Override
  public VectorValueSelector makeVectorValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    ColumnHolder holder = columnSelector.getColumnHolder(fieldSpec.columnName);
    if (holder == null) {
      return NilVectorSelector.create(offset);
    }
    BaseColumn theColumn = holder.getColumn();
    if (!(theColumn instanceof NestedDataComplexColumn)) {
      // not a nested column, but we can still try to coerce the values to the expected type of value selector if the
      // path is the root path
      if (fieldSpec.parts.isEmpty()) {
        // coerce string columns (a bit presumptuous in general, but in practice these are going to be string columns
        // ... revisit this if that ever changes)
        if (theColumn instanceof DictionaryEncodedColumn) {
          final VectorObjectSelector delegate = theColumn.makeVectorObjectSelector(offset);
          final ColumnType castTo = fieldSpec.expectedType != null ? fieldSpec.expectedType : ColumnType.DOUBLE;
          return ExpressionVectorSelectors.castObjectSelectorToNumeric(
              offset,
              columnName,
              delegate,
              holder.getCapabilities().toColumnType(),
              castTo
          );
        }
        // otherwise, just use the columns native vector value selector (this might explode if not natively numeric)
        return theColumn.makeVectorValueSelector(offset);
      }
      // array columns can also be handled if the path is a root level array element accessor
      if (isRootArrayElementPathAndArrayColumn(theColumn)) {
        final VariantColumn<?> arrayColumn = (VariantColumn<?>) theColumn;
        VectorObjectSelector arraySelector = arrayColumn.makeVectorObjectSelector(offset);
        final int elementNumber = ((NestedPathArrayElement) fieldSpec.parts.get(0)).getIndex();
        if (elementNumber < 0) {
          throw new IAE("Cannot make array element selector, negative array index not supported");
        }

        if (fieldSpec.expectedType != null && fieldSpec.expectedType.is(ValueType.LONG)) {
          return new ArrayElementLongVectorValueSelector(offset, arraySelector, elementNumber);
        } else if (fieldSpec.expectedType != null && fieldSpec.expectedType.is(ValueType.FLOAT)) {
          return new ArrayElementFloatVectorValueSelector(offset, arraySelector, elementNumber);
        } else {
          return new ArrayElementDoubleVectorValueSelector(offset, arraySelector, elementNumber);
        }
      }
      return NilVectorSelector.create(offset);
    }

    final NestedDataComplexColumn column = (NestedDataComplexColumn) theColumn;
    // if column is numeric, it has a vector value selector, so we can directly make a vector value selector
    // if we are missing an expectedType, then we've got nothing else to work with so try it anyway
    final ColumnType leastRestrictiveType = column.getFieldLogicalType(fieldSpec.parts);

    if (column.isNumeric(fieldSpec.parts) || fieldSpec.expectedType == null) {
      return column.makeVectorValueSelector(fieldSpec.parts, offset);
    }

    final VectorObjectSelector fieldSelector = column.makeVectorObjectSelector(fieldSpec.parts, offset);
    return ExpressionVectorSelectors.castObjectSelectorToNumeric(
        offset,
        columnName,
        fieldSelector,
        leastRestrictiveType,
        fieldSpec.expectedType
    );
  }

  @Nullable
  @Override
  public ColumnIndexSupplier getIndexSupplier(
      String columnName,
      ColumnIndexSelector indexSelector
  )
  {
    ColumnHolder holder = indexSelector.getColumnHolder(fieldSpec.columnName);
    if (holder == null) {
      return null;
    }
    BaseColumn theColumn = holder.getColumn();
    if (theColumn instanceof CompressedNestedDataComplexColumn) {
      final CompressedNestedDataComplexColumn<?> nestedColumn = (CompressedNestedDataComplexColumn<?>) theColumn;
      final ColumnIndexSupplier nestedColumnPathIndexSupplier = nestedColumn.getColumnIndexSupplier(fieldSpec.parts);
      if (nestedColumnPathIndexSupplier == null && fieldSpec.processFromRaw) {
        // if processing from raw, a non-exstent path from parts doesn't mean the path doesn't really exist
        // so fall back to no indexes
        return NoIndexesColumnIndexSupplier.getInstance();
      }
      if (fieldSpec.expectedType != null) {
        final Set<ColumnType> types = nestedColumn.getFieldTypes(fieldSpec.parts);
        // if the expected output type is numeric but not all of the input types are numeric, we might have additional
        // null values than what the null value bitmap is tracking, fall back to not using indexes
        if (fieldSpec.expectedType.isNumeric() && (types == null || types.stream().anyMatch(t -> !t.isNumeric()))) {
          return NoIndexesColumnIndexSupplier.getInstance();
        }
      }
      return nestedColumnPathIndexSupplier;
    }
    if (fieldSpec.parts.isEmpty()) {
      final ColumnIndexSupplier baseIndexSupplier = holder.getIndexSupplier();
      if (fieldSpec.expectedType != null) {
        if (theColumn instanceof NumericColumn) {
          return baseIndexSupplier;
        }
        if (theColumn instanceof NestedCommonFormatColumn) {
          final NestedCommonFormatColumn commonFormat = (NestedCommonFormatColumn) theColumn;
          if (fieldSpec.expectedType.isNumeric() && !commonFormat.getLogicalType().isNumeric()) {
            return NoIndexesColumnIndexSupplier.getInstance();
          }
        } else {
          return fieldSpec.expectedType.isNumeric() ? NoIndexesColumnIndexSupplier.getInstance() : baseIndexSupplier;
        }
      }
      return baseIndexSupplier;
    }
    if (isRootArrayElementPathAndArrayColumn(theColumn)) {
      // cannot use the array column index supplier directly, in the future array columns should expose a function
      // with a signature like 'getArrayElementIndexSupplier(int index)' to allow getting indexes for specific elements
      // if we want to support this stuff. Right now VariantArrayColumn doesn't actually retain enough information about
      // what positions the values are in to support doing anything cool here, so we just return 'no indexes'
      return NoIndexesColumnIndexSupplier.getInstance();
    }
    return null;
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    if (fieldSpec.processFromRaw) {
      // JSON_QUERY always returns a StructuredData
      return ColumnCapabilitiesImpl.createDefault()
                                   .setType(ColumnType.NESTED_DATA)
                                   .setHasMultipleValues(false)
                                   .setHasNulls(true);
    }
    // this should only be used for 'realtime' queries, so don't indicate that we are dictionary encoded or have indexes
    // from here
    return ColumnCapabilitiesImpl.createDefault()
                                 .setType(fieldSpec.expectedType != null ? fieldSpec.expectedType : ColumnType.STRING)
                                 .setHasNulls(true);
  }

  @Nullable
  @Override
  public ColumnCapabilities capabilities(ColumnInspector inspector, String columnName)
  {
    if (fieldSpec.processFromRaw) {
      if (fieldSpec.expectedType != null && fieldSpec.expectedType.isArray() && ColumnType.NESTED_DATA.equals(fieldSpec.expectedType.getElementType())) {
        // arrays of objects!
        return ColumnCapabilitiesImpl.createDefault()
                                     .setType(ColumnType.ofArray(ColumnType.NESTED_DATA))
                                     .setHasMultipleValues(false)
                                     .setHasNulls(true);
      }
      // JSON_QUERY always returns a StructuredData
      return ColumnCapabilitiesImpl.createDefault()
                                   .setType(ColumnType.NESTED_DATA)
                                   .setHasMultipleValues(false)
                                   .setHasNulls(true);
    }
    // ColumnInspector isn't really enough... we need the ability to read the complex column itself to examine
    // the nested fields type information to really be accurate here, so we rely on the expectedType to guide us
    final ColumnCapabilities capabilities = inspector.getColumnCapabilities(fieldSpec.columnName);

    if (capabilities != null) {
      // if the underlying column is a nested column (and persisted to disk, re: the dictionary encoded check)
      if (capabilities.is(ValueType.COMPLEX) &&
          capabilities.getComplexTypeName().equals(NestedDataComplexTypeSerde.TYPE_NAME) &&
          capabilities.isDictionaryEncoded().isTrue()) {
        final boolean useDictionary = fieldSpec.parts.isEmpty() || !(fieldSpec.parts.get(fieldSpec.parts.size() - 1) instanceof NestedPathArrayElement);
        return ColumnCapabilitiesImpl.createDefault()
                                     .setType(fieldSpec.expectedType != null ? fieldSpec.expectedType : ColumnType.STRING)
                                     .setDictionaryEncoded(useDictionary)
                                     .setDictionaryValuesSorted(useDictionary)
                                     .setDictionaryValuesUnique(useDictionary)
                                     .setHasBitmapIndexes(useDictionary)
                                     .setHasNulls(true);
      }
      // column is not nested, use underlying column capabilities, adjusted for expectedType as necessary
      if (fieldSpec.parts.isEmpty()) {
        ColumnCapabilitiesImpl copy = ColumnCapabilitiesImpl.copyOf(capabilities);
        if (fieldSpec.expectedType != null) {
          copy.setType(fieldSpec.expectedType);
          copy.setHasNulls(
              copy.hasNulls().or(ColumnCapabilities.Capable.of(fieldSpec.expectedType.getType() != capabilities.getType()))
          );
        }
        return copy;
      } else if (capabilities.isPrimitive()) {
        // path doesn't exist and column isn't nested, so effectively column doesn't exist
        return null;
      }
    }

    return capabilities(columnName);
  }

  @Override
  public List<String> requiredColumns()
  {
    return Collections.singletonList(fieldSpec.columnName);
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Nullable
  @Override
  public EquivalenceKey getEquivalanceKey()
  {
    return fieldSpec;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NestedFieldVirtualColumn that = (NestedFieldVirtualColumn) o;
    return outputName.equals(that.outputName) &&
           fieldSpec.equals(that.fieldSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(outputName, fieldSpec);
  }

  @Override
  public String toString()
  {
    return "NestedFieldVirtualColumn{" +
           "columnName='" + fieldSpec.columnName + '\'' +
           ", outputName='" + outputName + '\'' +
           ", typeHint='" + fieldSpec.expectedType + '\'' +
           ", pathParts='" + fieldSpec.parts + '\'' +
           ", allowFallback=" + fieldSpec.processFromRaw +
           '}';
  }

  /**
   * Returns true if json path is a root array element (for example '$[1]') and the column array column
   */
  private boolean isRootArrayElementPathAndArrayColumn(BaseColumn theColumn)
  {
    return fieldSpec.parts.size() == 1
           && fieldSpec.parts.get(0) instanceof NestedPathArrayElement
           && theColumn instanceof VariantColumn;
  }

  private VectorObjectSelector castVectorObjectSelectorIfNeeded(
      String columnName,
      ReadableVectorOffset offset,
      ColumnType leastRestrictiveType,
      VectorObjectSelector objectSelector
  )
  {
    if (fieldSpec.expectedType != null && !Objects.equals(fieldSpec.expectedType, leastRestrictiveType)) {
      return ExpressionVectorSelectors.castObject(
          offset,
          columnName,
          objectSelector,
          leastRestrictiveType,
          fieldSpec.expectedType
      );
    }
    return objectSelector;
  }

  private static class NestedFieldSpec implements EquivalenceKey
  {
    private final String columnName;
    @Nullable
    private final ColumnType expectedType;
    private final List<NestedPathPart> parts;
    private final boolean processFromRaw;

    private NestedFieldSpec(
        String columnName,
        @Nullable ColumnType expectedType,
        List<NestedPathPart> parts,
        boolean processFromRaw
    )
    {
      this.columnName = columnName;
      this.expectedType = expectedType;
      this.parts = parts;
      this.processFromRaw = processFromRaw;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedFieldSpec that = (NestedFieldSpec) o;
      return processFromRaw == that.processFromRaw
             && Objects.equals(columnName, that.columnName)
             && Objects.equals(expectedType, that.expectedType)
             && Objects.equals(parts, that.parts);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(columnName, expectedType, parts, processFromRaw);
    }
  }


  /**
   * Process the "raw" data to extract vectors of values with {@link NestedPathFinder#find(Object, List)}, wrapping the
   * result in {@link StructuredData}
   */
  public static final class RawFieldVectorObjectSelector implements VectorObjectSelector
  {
    private final VectorObjectSelector baseSelector;
    private final List<NestedPathPart> parts;
    private final Object[] vector;

    public RawFieldVectorObjectSelector(
        VectorObjectSelector baseSelector,
        List<NestedPathPart> parts
    )
    {
      this.baseSelector = baseSelector;
      this.parts = parts;
      this.vector = new Object[baseSelector.getMaxVectorSize()];
    }

    @Override
    public Object[] getObjectVector()
    {
      Object[] baseVector = baseSelector.getObjectVector();
      for (int i = 0; i < baseSelector.getCurrentVectorSize(); i++) {
        vector[i] = compute(baseVector[i]);
      }
      return vector;
    }

    @Override
    public int getMaxVectorSize()
    {
      return baseSelector.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return baseSelector.getCurrentVectorSize();
    }

    private Object compute(Object input)
    {
      StructuredData data = StructuredData.wrap(input);
      return StructuredData.wrap(NestedPathFinder.find(data == null ? null : data.getValue(), parts));
    }
  }

  /**
   * Create a {@link DimensionSelector} for a nested field on top of a {@link ColumnValueSelector}
   */
  public static final class FieldDimensionSelector extends BaseSingleValueDimensionSelector
  {
    private final ColumnValueSelector<?> valueSelector;

    public FieldDimensionSelector(ColumnValueSelector<?> valueSelector)
    {
      this.valueSelector = valueSelector;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("valueSelector", valueSelector);
    }

    @Nullable
    @Override
    protected String getValue()
    {
      Object val = valueSelector.getObject();
      if (val == null || val instanceof String) {
        return (String) val;
      }
      if (val instanceof Object[]) {
        Object[] arrayVal = (Object[]) val;
        if (arrayVal.length == 1) {
          return String.valueOf(arrayVal[0]);
        }
        return null;
      }
      return String.valueOf(val);
    }
  }

  /**
   * {@link DimensionSelector} that provides implicit numeric casting when used as a value selector, trying best effort
   * to implement {@link #getLong()}, {@link #getDouble()}, {@link #getFloat()}, {@link #isNull()} on top of some
   * other {@link DimensionSelector}.
   * <p>
   * This is used as a fall-back when making a selector and the underlying column is NOT a
   * {@link NestedDataComplexColumn}, whose field {@link DimensionSelector} natively implement this behavior.
   */
  private static final class BestEffortCastingValueSelector implements DimensionSelector
  {
    private final DimensionSelector baseSelector;

    public BestEffortCastingValueSelector(DimensionSelector baseSelector)
    {
      this.baseSelector = baseSelector;
    }

    @Override
    public IndexedInts getRow()
    {
      return baseSelector.getRow();
    }

    @Override
    public ValueMatcher makeValueMatcher(@Nullable String value)
    {
      return baseSelector.makeValueMatcher(value);
    }

    @Override
    public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
    {
      return baseSelector.makeValueMatcher(predicateFactory);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      baseSelector.inspectRuntimeShape(inspector);
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return baseSelector.getObject();
    }

    @Override
    public Class<?> classOfObject()
    {
      return baseSelector.classOfObject();
    }

    @Override
    public int getValueCardinality()
    {
      return baseSelector.getValueCardinality();
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      return baseSelector.lookupName(id);
    }

    @Nullable
    @Override
    public ByteBuffer lookupNameUtf8(int id)
    {
      return baseSelector.lookupNameUtf8(id);
    }

    @Override
    public boolean supportsLookupNameUtf8()
    {
      return baseSelector.supportsLookupNameUtf8();
    }

    @Override
    public float getFloat()
    {
      final IndexedInts row = getRow();
      if (row.size() != 1) {
        return 0f;
      }
      return Numbers.tryParseFloat(lookupName(row.get(0)), 0f);
    }

    @Override
    public double getDouble()
    {
      final IndexedInts row = getRow();
      if (row.size() != 1) {
        return 0.0;
      }
      return Numbers.tryParseDouble(lookupName(row.get(0)), 0.0);
    }

    @Override
    public long getLong()
    {
      final IndexedInts row = getRow();
      if (row.size() != 1) {
        return 0L;
      }
      return Numbers.tryParseLong(lookupName(row.get(0)), 0L);
    }

    @Override
    public boolean isNull()
    {
      final IndexedInts row = getRow();
      if (row.size() != 1) {
        return true;
      }
      final String s = lookupName(row.get(0));
      return s == null || Doubles.tryParse(s) == null;
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return baseSelector.nameLookupPossibleInAdvance();
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return baseSelector.idLookup();
    }
  }

  /**
   * {@link BaseLongVectorValueSelector} for selecting a specific element of an array type column
   * {@link VectorObjectSelector}
   */
  private static final class ArrayElementLongVectorValueSelector extends BaseLongVectorValueSelector
  {
    private final long[] longs;
    private final boolean[] nulls;
    private final VectorObjectSelector arraySelector;
    private final int elementNumber;
    private int id;

    public ArrayElementLongVectorValueSelector(
        ReadableVectorOffset offset,
        VectorObjectSelector arraySelector,
        int elementNumber
    )
    {
      super(offset);
      this.arraySelector = arraySelector;
      this.elementNumber = elementNumber;
      longs = new long[offset.getMaxVectorSize()];
      nulls = new boolean[offset.getMaxVectorSize()];
      id = ReadableVectorInspector.NULL_ID;
    }

    private void computeNumbers()
    {
      if (offset.getId() != id) {
        final Object[] maybeArrays = arraySelector.getObjectVector();
        for (int i = 0; i < arraySelector.getCurrentVectorSize(); i++) {
          Object maybeArray = maybeArrays[i];
          if (maybeArray instanceof Object[]) {
            Object[] anArray = (Object[]) maybeArray;
            if (elementNumber < anArray.length) {
              if (anArray[elementNumber] instanceof Number) {
                Number n = (Number) anArray[elementNumber];
                longs[i] = n.longValue();
                nulls[i] = false;
              } else {
                Double d = anArray[elementNumber] instanceof String
                           ? Doubles.tryParse((String) anArray[elementNumber])
                           : null;
                if (d != null) {
                  longs[i] = d.longValue();
                  nulls[i] = false;
                } else {
                  longs[i] = 0L;
                  nulls[i] = true;
                }
              }
            } else {
              nullElement(i);
            }
          } else {
            // not an array?
            nullElement(i);
          }
        }
        id = offset.getId();
      }
    }

    private void nullElement(int i)
    {
      longs[i] = 0L;
      nulls[i] = true;
    }

    @Override
    public long[] getLongVector()
    {
      if (offset.getId() != id) {
        computeNumbers();
      }
      return longs;
    }

    @Nullable
    @Override
    public boolean[] getNullVector()
    {
      if (offset.getId() != id) {
        computeNumbers();
      }
      return nulls;
    }
  }

  /**
   * {@link BaseFloatVectorValueSelector} for selecting a specific element of an array type column
   * {@link VectorObjectSelector}
   */
  private static final class ArrayElementFloatVectorValueSelector extends BaseFloatVectorValueSelector
  {
    private final float[] floats;
    private final boolean[] nulls;
    private final VectorObjectSelector arraySelector;
    private final int elementNumber;
    private int id;

    public ArrayElementFloatVectorValueSelector(
        ReadableVectorOffset offset,
        VectorObjectSelector arraySelector,
        int elementNumber
    )
    {
      super(offset);
      this.arraySelector = arraySelector;
      this.elementNumber = elementNumber;
      floats = new float[offset.getMaxVectorSize()];
      nulls = new boolean[offset.getMaxVectorSize()];
      id = ReadableVectorInspector.NULL_ID;
    }

    private void computeNumbers()
    {
      if (offset.getId() != id) {
        final Object[] maybeArrays = arraySelector.getObjectVector();
        for (int i = 0; i < arraySelector.getCurrentVectorSize(); i++) {
          Object maybeArray = maybeArrays[i];
          if (maybeArray instanceof Object[]) {
            Object[] anArray = (Object[]) maybeArray;
            if (elementNumber < anArray.length) {
              if (anArray[elementNumber] instanceof Number) {
                Number n = (Number) anArray[elementNumber];
                floats[i] = n.floatValue();
                nulls[i] = false;
              } else {
                Double d = anArray[elementNumber] instanceof String
                           ? Doubles.tryParse((String) anArray[elementNumber])
                           : null;
                if (d != null) {
                  floats[i] = d.floatValue();
                  nulls[i] = false;
                } else {
                  nullElement(i);
                }
              }
            } else {
              nullElement(i);
            }
          } else {
            // not an array?
            nullElement(i);
          }
        }
        id = offset.getId();
      }
    }

    private void nullElement(int i)
    {
      floats[i] = 0f;
      nulls[i] = true;
    }

    @Override
    public float[] getFloatVector()
    {
      if (offset.getId() != id) {
        computeNumbers();
      }
      return floats;
    }

    @Override
    public boolean[] getNullVector()
    {
      if (offset.getId() != id) {
        computeNumbers();
      }
      return nulls;
    }
  }

  /**
   * {@link BaseDoubleVectorValueSelector} for selecting a specific element of an array type column
   * {@link VectorObjectSelector}
   */
  private static final class ArrayElementDoubleVectorValueSelector extends BaseDoubleVectorValueSelector
  {
    private final double[] doubles;
    private final boolean[] nulls;
    private final VectorObjectSelector arraySelector;
    private final int elementNumber;
    private int id;

    public ArrayElementDoubleVectorValueSelector(
        ReadableVectorOffset offset,
        VectorObjectSelector arraySelector,
        int elementNumber
    )
    {
      super(offset);
      this.arraySelector = arraySelector;
      this.elementNumber = elementNumber;
      doubles = new double[offset.getMaxVectorSize()];
      nulls = new boolean[offset.getMaxVectorSize()];
      id = ReadableVectorInspector.NULL_ID;
    }

    private void computeNumbers()
    {
      if (offset.getId() != id) {
        final Object[] maybeArrays = arraySelector.getObjectVector();
        for (int i = 0; i < arraySelector.getCurrentVectorSize(); i++) {
          Object maybeArray = maybeArrays[i];
          if (maybeArray instanceof Object[]) {
            Object[] anArray = (Object[]) maybeArray;
            if (elementNumber < anArray.length) {
              if (anArray[elementNumber] instanceof Number) {
                Number n = (Number) anArray[elementNumber];
                doubles[i] = n.doubleValue();
                nulls[i] = false;
              } else {
                Double d = anArray[elementNumber] instanceof String
                           ? Doubles.tryParse((String) anArray[elementNumber])
                           : null;
                if (d != null) {
                  doubles[i] = d;
                  nulls[i] = false;
                } else {
                  nullElement(i);
                }
              }
            } else {
              nullElement(i);
            }
          } else {
            // not an array?
            nullElement(i);
          }
        }
        id = offset.getId();
      }
    }

    private void nullElement(int i)
    {
      doubles[i] = 0.0;
      nulls[i] = true;
    }

    @Override
    public double[] getDoubleVector()
    {
      if (offset.getId() != id) {
        computeNumbers();
      }
      return doubles;
    }

    @Override
    public boolean[] getNullVector()
    {
      if (offset.getId() != id) {
        computeNumbers();
      }
      return nulls;
    }
  }

  /**
   * {@link VectorObjectSelector} for selecting a specific element of an array type column
   * {@link VectorObjectSelector}
   */
  private static final class ArrayElementVectorObjectSelector implements VectorObjectSelector
  {
    private final Object[] elements;
    private final VectorObjectSelector arraySelector;
    private final ReadableVectorOffset offset;
    private final int elementNumber;
    private final ExpressionType elementType;
    private final ExpressionType castTo;
    private int id;

    public ArrayElementVectorObjectSelector(
        VectorObjectSelector arraySelector,
        ReadableVectorOffset offset,
        int elementNumber,
        ExpressionType elementType,
        ExpressionType castTo
    )
    {
      this.arraySelector = arraySelector;
      this.offset = offset;
      this.elementNumber = elementNumber;
      this.elementType = elementType;
      this.castTo = castTo;
      elements = new Object[arraySelector.getMaxVectorSize()];
      id = ReadableVectorInspector.NULL_ID;
    }

    @Override
    public Object[] getObjectVector()
    {
      if (offset.getId() != id) {
        final Object[] delegate = arraySelector.getObjectVector();
        for (int i = 0; i < arraySelector.getCurrentVectorSize(); i++) {
          Object maybeArray = delegate[i];
          if (maybeArray instanceof Object[]) {
            Object[] anArray = (Object[]) maybeArray;
            if (elementNumber < anArray.length) {
              elements[i] = ExprEval.ofType(elementType, anArray[elementNumber]).castTo(castTo).value();
            } else {
              elements[i] = null;
            }
          } else {
            elements[i] = null;
          }
        }
        id = offset.getId();
      }
      return elements;
    }

    @Override
    public int getMaxVectorSize()
    {
      return arraySelector.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return arraySelector.getCurrentVectorSize();
    }
  }

  /**
   * {@link ColumnValueSelector} for selecting a specific element of an array type column {@link ColumnValueSelector}
   */
  private static final class ArrayElementColumnValueSelector implements ColumnValueSelector<Object>
  {
    private final ColumnValueSelector<?> arraySelector;
    private final int elementNumber;

    public ArrayElementColumnValueSelector(ColumnValueSelector<?> arraySelector, int elementNumber)
    {
      this.arraySelector = arraySelector;
      this.elementNumber = elementNumber;
    }

    @Override
    public boolean isNull()
    {
      Object o = getObject();
      return !(o instanceof Number);
    }

    @Override
    public long getLong()
    {
      Object o = getObject();
      return o instanceof Number ? ((Number) o).longValue() : 0L;
    }

    @Override
    public float getFloat()
    {
      Object o = getObject();
      return o instanceof Number ? ((Number) o).floatValue() : 0f;
    }

    @Override
    public double getDouble()
    {
      Object o = getObject();
      return o instanceof Number ? ((Number) o).doubleValue() : 0.0;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      arraySelector.inspectRuntimeShape(inspector);
    }

    @Nullable
    @Override
    public Object getObject()
    {
      Object o = arraySelector.getObject();
      if (o instanceof Object[]) {
        Object[] array = (Object[]) o;
        if (elementNumber < array.length) {
          return array[elementNumber];
        }
      }
      return null;
    }

    @Override
    public Class<?> classOfObject()
    {
      return Object.class;
    }
  }
}
