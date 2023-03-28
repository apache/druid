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
import com.google.common.base.Predicate;
import com.google.common.primitives.Doubles;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
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
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.column.ValueTypes;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.nested.CompressedNestedDataComplexColumn;
import org.apache.druid.segment.nested.NestedDataComplexColumn;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.nested.NestedFieldDictionaryEncodedColumn;
import org.apache.druid.segment.nested.NestedPathArrayElement;
import org.apache.druid.segment.nested.NestedPathFinder;
import org.apache.druid.segment.nested.NestedPathPart;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.vector.BaseDoubleVectorValueSelector;
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

/**
 * Optimized virtual column that can make direct selectors into a {@link NestedDataComplexColumn} or any associated
 * nested fields ({@link NestedFieldDictionaryEncodedColumn}) including using
 * their indexes.
 * <p>
 * This virtual column is used for the SQL operators JSON_VALUE (if {@link #processFromRaw} is set to false) or
 * JSON_QUERY (if it is true), and accepts 'JSONPath' or 'jq' syntax string representations of paths, or a parsed
 * list of {@link NestedPathPart} in order to determine what should be selected from the column.
 * <p>
 * Type information for nested fields is completely absent in the SQL planner, so it guesses the best it can to set
 * {@link #expectedType} from the context of how something is being used, e.g. an aggregators default type or an
 * explicit cast, or, if using the 'RETURNING' syntax which explicitly specifies type. This might not be the same as
 * if it had actual type information, but, we try to stick with whatever we chose there to do the best we can for now.
 * <p>
 * Since {@link #capabilities(ColumnInspector, String)} is determined by the {@link #expectedType}, the results will
 * be best effor cast to the expected type if the column is not natively the expected type so that this column can
 * fulfill the contract of the type of selector that is likely to be created to read this column.
 */
public class NestedFieldVirtualColumn implements VirtualColumn
{
  private final String columnName;
  private final String outputName;
  @Nullable
  private final ColumnType expectedType;
  private final List<NestedPathPart> parts;
  private final boolean processFromRaw;

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
    this.columnName = columnName;
    this.outputName = outputName;
    if (path != null) {
      Preconditions.checkArgument(parts == null, "Cannot define both 'path' and 'pathParts'");
    } else if (parts == null) {
      throw new IllegalArgumentException("Must define exactly one of 'path' or 'pathParts'");
    }

    if (parts != null) {
      this.parts = parts;
    } else {
      boolean isInputJq = useJqSyntax != null && useJqSyntax;
      this.parts = isInputJq ? NestedPathFinder.parseJqPath(path) : NestedPathFinder.parseJsonPath(path);
    }
    boolean hasNegative = false;
    for (NestedPathPart part : this.parts) {
      if (part instanceof NestedPathArrayElement) {
        NestedPathArrayElement elementPart = (NestedPathArrayElement) part;
        if (elementPart.getIndex() < 0) {
          hasNegative = true;
          break;
        }
      }
    }
    this.hasNegativeArrayIndex = hasNegative;
    this.expectedType = expectedType;
    this.processFromRaw = processFromRaw == null ? false : processFromRaw;
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
      ColumnType expectedType
  )
  {
    this(columnName, outputName, expectedType, null, null, path, false);
  }

  @Override
  public byte[] getCacheKey()
  {
    final String partsString = NestedPathFinder.toNormalizedJsonPath(parts);
    return new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_USER_DEFINED).appendString("nested-field")
                                                                                   .appendString(outputName)
                                                                                   .appendString(columnName)
                                                                                   .appendString(partsString)
                                                                                   .appendBoolean(processFromRaw)
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
    return columnName;
  }

  @JsonProperty("pathParts")
  public List<NestedPathPart> getPathParts()
  {
    return parts;
  }

  @JsonProperty
  public ColumnType getExpectedType()
  {
    return expectedType;
  }

  @JsonProperty
  public boolean isProcessFromRaw()
  {
    return processFromRaw;
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
    // this column value selector is used for realtime queries, so we always process StructuredData
    final ColumnValueSelector<?> baseSelector = factory.makeColumnValueSelector(this.columnName);

    // processFromRaw is true that means JSON_QUERY, which can return partial results, otherwise this virtual column
    // is JSON_VALUE which only returns literals, so use the literal value selector instead
    return processFromRaw
           ? new RawFieldColumnSelector(baseSelector, parts)
           : new RawFieldLiteralColumnValueSelector(baseSelector, parts);
  }

  @Nullable
  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    ColumnHolder holder = columnSelector.getColumnHolder(columnName);
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
      return column.makeDimensionSelector(parts, offset, extractionFn);
    }

    // not a nested column, but we can still do stuff if the path is the 'root', indicated by an empty path parts
    if (parts.isEmpty()) {
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
    ColumnHolder holder = columnSelector.getColumnHolder(this.columnName);
    if (holder == null) {
      return NilColumnValueSelector.instance();
    }
    BaseColumn theColumn = holder.getColumn();

    if (processFromRaw || hasNegativeArrayIndex) {
      // if the path has negative array elements, or has set the flag to process 'raw' values explicitly (JSON_QUERY),
      // then we use the 'raw' processing of the RawFieldColumnSelector/RawFieldLiteralColumnValueSelector created
      // with the column selector factory instead of using the optimized nested field column
      return null;
    }

    // "JSON_VALUE", which only returns literals, on a NestedDataComplexColumn, so we can use the fields value selector
    if (theColumn instanceof NestedDataComplexColumn) {
      final NestedDataComplexColumn column = (NestedDataComplexColumn) theColumn;
      return column.makeColumnValueSelector(parts, offset);
    }

    // not a nested column, but we can still do stuff if the path is the 'root', indicated by an empty path parts
    if (parts.isEmpty()) {
      // dictionary encoded columns do not typically implement the value selector methods (getLong, getDouble, getFloat)
      // so we want to wrap their selector in a "best effort" casting selector to implement them
      if (theColumn instanceof DictionaryEncodedColumn) {
        final DictionaryEncodedColumn<?> column = (DictionaryEncodedColumn<?>) theColumn;
        return new BestEffortCastingValueSelector(column.makeDimensionSelector(offset, null));
      }
      // otherwise it is probably cool to pass through the value selector directly, if numbers make sense the selector
      // very likely implemented them, and everyone implements getObject if not
      return theColumn.makeColumnValueSelector(offset);
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
    ColumnHolder holder = columnSelector.getColumnHolder(columnName);
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
      return column.makeSingleValueDimensionVectorSelector(parts, offset);
    }

    // not a nested column, but we can still do stuff if the path is the 'root', indicated by an empty path parts
    if (parts.isEmpty()) {
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
    ColumnHolder holder = columnSelector.getColumnHolder(this.columnName);
    if (holder == null) {
      return NilVectorSelector.create(offset);
    }
    BaseColumn theColumn = holder.getColumn();

    // processFromRaw is true, that means JSON_QUERY, which can return partial results, otherwise this virtual column
    // is JSON_VALUE which only returns literals, so we can use the nested columns value selector
    if (theColumn instanceof NestedDataComplexColumn) {
      final NestedDataComplexColumn column = (NestedDataComplexColumn) theColumn;
      if (processFromRaw) {
        return new RawFieldVectorObjectSelector(column.makeVectorObjectSelector(offset), parts);
      }
      return column.makeVectorObjectSelector(parts, offset);
    }
    // not a nested column, but we can still do stuff if the path is the 'root', indicated by an empty path parts
    if (parts.isEmpty()) {
      ColumnCapabilities capabilities = holder.getCapabilities();
      // expectedType shouldn't possibly be null if we are being asked for an object selector and the underlying column
      // is numeric, else we would have been asked for a value selector
      Preconditions.checkArgument(expectedType != null, "Asked for a VectorObjectSelector on a numeric column, 'expectedType' must not be null");
      if (capabilities.isNumeric()) {
        return ExpressionVectorSelectors.castValueSelectorToObject(
            offset,
            this.columnName,
            theColumn.makeVectorValueSelector(offset),
            capabilities.toColumnType(),
            expectedType
        );
      }
      return theColumn.makeVectorObjectSelector(offset);
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
    ColumnHolder holder = columnSelector.getColumnHolder(this.columnName);
    if (holder == null) {
      return NilVectorSelector.create(offset);
    }
    BaseColumn theColumn = holder.getColumn();
    if (!(theColumn instanceof NestedDataComplexColumn)) {

      if (parts.isEmpty()) {
        ColumnCapabilities capabilities = holder.getCapabilities();
        if (theColumn instanceof DictionaryEncodedColumn) {
          return ExpressionVectorSelectors.castObjectSelectorToNumeric(
              offset,
              this.columnName,
              theColumn.makeVectorObjectSelector(offset),
              capabilities.toColumnType(),
              expectedType
          );
        }
        return theColumn.makeVectorValueSelector(offset);
      }
      return NilVectorSelector.create(offset);
    }

    final NestedDataComplexColumn column = (NestedDataComplexColumn) theColumn;
    // if column is numeric, it has a vector value selector, so we can directly make a vector value selector
    // if we are missing an expectedType, then we've got nothing else to work with so try it anyway
    if (column.isNumeric(parts) || expectedType == null) {
      return column.makeVectorValueSelector(parts, offset);
    }

    final VectorObjectSelector objectSelector = column.makeVectorObjectSelector(parts, offset);
    if (expectedType.is(ValueType.LONG)) {
      return new BaseLongVectorValueSelector(offset)
      {
        private final long[] longVector = new long[offset.getMaxVectorSize()];

        @Nullable
        private boolean[] nullVector = null;
        private int id = ReadableVectorInspector.NULL_ID;

        @Override
        public long[] getLongVector()
        {
          computeVectorsIfNeeded();
          return longVector;
        }

        @Nullable
        @Override
        public boolean[] getNullVector()
        {
          computeVectorsIfNeeded();
          return nullVector;
        }

        private void computeVectorsIfNeeded()
        {
          if (id == offset.getId()) {
            return;
          }
          id = offset.getId();
          final Object[] vals = objectSelector.getObjectVector();
          for (int i = 0; i < objectSelector.getCurrentVectorSize(); i++) {
            Object v = vals[i];
            if (v == null) {
              if (nullVector == null) {
                nullVector = new boolean[objectSelector.getMaxVectorSize()];
              }
              longVector[i] = 0L;
              nullVector[i] = NullHandling.sqlCompatible();
            } else {
              Long l;
              if (v instanceof Number) {
                l = ((Number) v).longValue();
              } else {
                l = GuavaUtils.tryParseLong(String.valueOf(v));
              }
              if (l != null) {
                longVector[i] = l;
                if (nullVector != null) {
                  nullVector[i] = false;
                }
              } else {
                if (nullVector == null) {
                  nullVector = new boolean[objectSelector.getMaxVectorSize()];
                }
                longVector[i] = 0L;
                nullVector[i] = NullHandling.sqlCompatible();
              }
            }
          }
        }
      };
    } else {
      // treat anything else as double
      return new BaseDoubleVectorValueSelector(offset)
      {
        private final double[] doubleVector = new double[offset.getMaxVectorSize()];

        @Nullable
        private boolean[] nullVector = null;
        private int id = ReadableVectorInspector.NULL_ID;

        @Override
        public double[] getDoubleVector()
        {
          computeVectorsIfNeeded();
          return doubleVector;
        }

        @Nullable
        @Override
        public boolean[] getNullVector()
        {
          computeVectorsIfNeeded();
          return nullVector;
        }

        private void computeVectorsIfNeeded()
        {
          if (id == offset.getId()) {
            return;
          }
          id = offset.getId();
          final Object[] vals = objectSelector.getObjectVector();
          for (int i = 0; i < objectSelector.getCurrentVectorSize(); i++) {
            Object v = vals[i];
            if (v == null) {
              if (nullVector == null) {
                nullVector = new boolean[objectSelector.getMaxVectorSize()];
              }
              doubleVector[i] = 0.0;
              nullVector[i] = NullHandling.sqlCompatible();
            } else {
              Double d;
              if (v instanceof Number) {
                d = ((Number) v).doubleValue();
              } else {
                d = Doubles.tryParse(String.valueOf(v));
              }
              if (d != null) {
                doubleVector[i] = d;
                if (nullVector != null) {
                  nullVector[i] = false;
                }
              } else {
                if (nullVector == null) {
                  nullVector = new boolean[objectSelector.getMaxVectorSize()];
                }
                doubleVector[i] = 0.0;
                nullVector[i] = NullHandling.sqlCompatible();
              }
            }
          }
        }
      };
    }
  }

  @Nullable
  @Override
  public ColumnIndexSupplier getIndexSupplier(
      String columnName,
      ColumnSelector selector
  )
  {
    ColumnHolder holder = selector.getColumnHolder(this.columnName);
    if (holder == null) {
      return null;
    }
    BaseColumn theColumn = holder.getColumn();
    if (theColumn instanceof CompressedNestedDataComplexColumn) {
      return ((CompressedNestedDataComplexColumn<?>) theColumn).getColumnIndexSupplier(parts);
    }
    if (parts.isEmpty()) {
      return holder.getIndexSupplier();
    }
    return null;
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    if (processFromRaw) {
      // JSON_QUERY always returns a StructuredData
      return ColumnCapabilitiesImpl.createDefault()
                                   .setType(NestedDataComplexTypeSerde.TYPE)
                                   .setHasMultipleValues(false)
                                   .setHasNulls(true);
    }
    // this should only be used for 'realtime' queries, so don't indicate that we are dictionary encoded or have indexes
    // from here
    return ColumnCapabilitiesImpl.createDefault()
                                 .setType(expectedType != null ? expectedType : ColumnType.STRING)
                                 .setHasNulls(expectedType == null || !expectedType.isNumeric() || (expectedType.isNumeric() && NullHandling.sqlCompatible()));
  }

  @Nullable
  @Override
  public ColumnCapabilities capabilities(ColumnInspector inspector, String columnName)
  {
    if (processFromRaw) {
      // JSON_QUERY always returns a StructuredData
      return ColumnCapabilitiesImpl.createDefault()
                                   .setType(NestedDataComplexTypeSerde.TYPE)
                                   .setHasMultipleValues(false)
                                   .setHasNulls(true);
    }
    // ColumnInspector isn't really enough... we need the ability to read the complex column itself to examine
    // the nested fields type information to really be accurate here, so we rely on the expectedType to guide us
    final ColumnCapabilities capabilities = inspector.getColumnCapabilities(this.columnName);

    if (capabilities != null) {
      // if the underlying column is a nested column (and persisted to disk, re: the dictionary encoded check)
      if (capabilities.is(ValueType.COMPLEX) &&
          capabilities.getComplexTypeName().equals(NestedDataComplexTypeSerde.TYPE_NAME) &&
          capabilities.isDictionaryEncoded().isTrue()) {
        final boolean useDictionary = parts.isEmpty() || !(parts.get(parts.size() - 1) instanceof NestedPathArrayElement);
        return ColumnCapabilitiesImpl.createDefault()
                                     .setType(expectedType != null ? expectedType : ColumnType.STRING)
                                     .setDictionaryEncoded(useDictionary)
                                     .setDictionaryValuesSorted(useDictionary)
                                     .setDictionaryValuesUnique(useDictionary)
                                     .setHasBitmapIndexes(useDictionary)
                                     .setHasNulls(expectedType == null || (expectedType.isNumeric()
                                                                           && NullHandling.sqlCompatible()));
      }
      // column is not nested, use underlying column capabilities, adjusted for expectedType as necessary
      if (parts.isEmpty()) {
        ColumnCapabilitiesImpl copy = ColumnCapabilitiesImpl.copyOf(capabilities);
        if (expectedType != null) {
          copy.setType(expectedType);
          copy.setHasNulls(
              copy.hasNulls().or(ColumnCapabilities.Capable.of(expectedType.getType() != capabilities.getType()))
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
    return Collections.singletonList(columnName);
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
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
    return columnName.equals(that.columnName) &&
           outputName.equals(that.outputName) &&
           parts.equals(that.parts) &&
           Objects.equals(expectedType, that.expectedType) &&
           processFromRaw == that.processFromRaw;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnName, parts, outputName, expectedType, processFromRaw);
  }

  @Override
  public String toString()
  {
    return "NestedFieldVirtualColumn{" +
           "columnName='" + columnName + '\'' +
           ", outputName='" + outputName + '\'' +
           ", typeHint='" + expectedType + '\'' +
           ", pathParts='" + parts + '\'' +
           ", allowFallback=" + processFromRaw +
           '}';
  }

  /**
   * Process the "raw" data to extract literals with {@link NestedPathFinder#findLiteral(Object, List)}. Like
   * {@link RawFieldColumnSelector} but only literals and does not wrap the results in {@link StructuredData}.
   * <p>
   * This is used as a selector on realtime data when the native field columns are not available.
   */
  public static class RawFieldLiteralColumnValueSelector extends RawFieldColumnSelector
  {
    public RawFieldLiteralColumnValueSelector(ColumnValueSelector baseSelector, List<NestedPathPart> parts)
    {
      super(baseSelector, parts);
    }

    @Override
    public double getDouble()
    {
      Object o = getObject();
      return Numbers.tryParseDouble(o, 0.0);
    }

    @Override
    public float getFloat()
    {
      Object o = getObject();
      return Numbers.tryParseFloat(o, 0.0f);
    }

    @Override
    public long getLong()
    {
      Object o = getObject();
      return Numbers.tryParseLong(o, 0L);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("baseSelector", baseSelector);
      inspector.visit("parts", parts);
    }

    @Override
    public boolean isNull()
    {
      Object o = getObject();
      return !(o instanceof Number || (o instanceof String && Doubles.tryParse((String) o) != null));
    }

    @Nullable
    @Override
    public Object getObject()
    {
      StructuredData data = StructuredData.wrap(baseSelector.getObject());
      return NestedPathFinder.findLiteral(data == null ? null : data.getValue(), parts);
    }

    @Override
    public Class<?> classOfObject()
    {
      return Object.class;
    }
  }

  /**
   * Process the "raw" data to extract values with {@link NestedPathFinder#find(Object, List)}, wrapping the result in
   * {@link StructuredData}
   */
  public static class RawFieldColumnSelector implements ColumnValueSelector<Object>
  {
    protected final ColumnValueSelector baseSelector;
    protected final List<NestedPathPart> parts;

    public RawFieldColumnSelector(ColumnValueSelector baseSelector, List<NestedPathPart> parts)
    {
      this.baseSelector = baseSelector;
      this.parts = parts;
    }

    @Override
    public double getDouble()
    {
      StructuredData data = (StructuredData) getObject();
      if (data != null) {
        return Numbers.tryParseDouble(data.getValue(), 0.0);
      }
      return 0.0;
    }

    @Override
    public float getFloat()
    {
      StructuredData data = (StructuredData) getObject();
      if (data != null) {
        return Numbers.tryParseFloat(data.getValue(), 0f);
      }
      return 0f;
    }

    @Override
    public long getLong()
    {
      StructuredData data = (StructuredData) getObject();
      if (data != null) {
        return Numbers.tryParseLong(data.getValue(), 0L);
      }
      return 0L;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("baseSelector", baseSelector);
      inspector.visit("parts", parts);
    }

    @Override
    public boolean isNull()
    {
      StructuredData data = (StructuredData) getObject();
      if (data == null) {
        return true;
      }
      Object o = data.getValue();
      return !(o instanceof Number || (o instanceof String && Doubles.tryParse((String) o) != null));
    }

    @Nullable
    @Override
    public Object getObject()
    {
      StructuredData data = StructuredData.wrap(baseSelector.getObject());
      return StructuredData.wrap(NestedPathFinder.find(data == null ? null : data.getValue(), parts));
    }

    @Override
    public Class<?> classOfObject()
    {
      return Object.class;
    }
  }

  /**
   * Process the "raw" data to extract vectors of values with {@link NestedPathFinder#find(Object, List)}, wrapping the
   * result in {@link StructuredData}
   */
  public static class RawFieldVectorObjectSelector implements VectorObjectSelector
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

  public static class FieldDimensionSelector extends BaseSingleValueDimensionSelector
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
  private static class BestEffortCastingValueSelector implements DimensionSelector
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
    public ValueMatcher makeValueMatcher(Predicate<String> predicate)
    {
      return baseSelector.makeValueMatcher(predicate);
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
      return Doubles.tryParse(lookupName(row.get(0))) == null;
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
}
