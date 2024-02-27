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

package org.apache.druid.segment;

import com.google.common.primitives.Doubles;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ColumnTypeFactory;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRowHolder;
import org.apache.druid.segment.nested.FieldTypeInfo;
import org.apache.druid.segment.nested.NestedPathFinder;
import org.apache.druid.segment.nested.NestedPathPart;
import org.apache.druid.segment.nested.SortedValueDictionary;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.nested.StructuredDataProcessor;
import org.apache.druid.segment.nested.ValueDictionary;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class AutoTypeColumnIndexer implements DimensionIndexer<StructuredData, StructuredData, StructuredData>
{
  /**
   * have we seen any null values?
   */
  protected volatile boolean hasNulls = false;
  /**
   * Have we seen any objects? Empty objects in particular are sneaky, they don't have any nested paths, so we also
   * broadly track if we have processed any objects {@link StructuredDataProcessor.ProcessResults#hasObjects()}
   */
  protected volatile boolean hasNestedData = false;
  protected volatile boolean isConstant = true;
  @Nullable
  protected volatile Object constantValue = null;
  private volatile boolean firstRow = true;

  protected SortedMap<String, FieldIndexer> fieldIndexers = new TreeMap<>();
  protected final ValueDictionary globalDictionary = new ValueDictionary();

  protected int estimatedFieldKeySize = 0;

  private final String columnName;
  @Nullable
  protected final ColumnType castToType;
  @Nullable
  protected final ExpressionType castToExpressionType;


  protected final StructuredDataProcessor indexerProcessor = new StructuredDataProcessor()
  {
    @Override
    public ProcessedValue<?> processField(ArrayList<NestedPathPart> fieldPath, @Nullable Object fieldValue)
    {
      // null value is always added to the global dictionary as id 0, so we can ignore them here
      if (fieldValue != null) {
        final String fieldName = NestedPathFinder.toNormalizedJsonPath(fieldPath);
        ExprEval<?> eval = ExprEval.bestEffortOf(fieldValue);
        FieldIndexer fieldIndexer = fieldIndexers.get(fieldName);
        if (fieldIndexer == null) {
          estimatedFieldKeySize += StructuredDataProcessor.estimateStringSize(fieldName);
          fieldIndexer = new FieldIndexer(globalDictionary);
          fieldIndexers.put(fieldName, fieldIndexer);
        }
        return fieldIndexer.processValue(eval);
      }
      return ProcessedValue.NULL_LITERAL;
    }

    @Nullable
    @Override
    public ProcessedValue<?> processArrayField(
        ArrayList<NestedPathPart> fieldPath,
        @Nullable List<?> array
    )
    {
      final ExprEval<?> eval = ExprEval.bestEffortArray(array);
      if (eval.type().isPrimitiveArray()) {
        final String fieldName = NestedPathFinder.toNormalizedJsonPath(fieldPath);
        FieldIndexer fieldIndexer = fieldIndexers.get(fieldName);
        if (fieldIndexer == null) {
          estimatedFieldKeySize += StructuredDataProcessor.estimateStringSize(fieldName);
          fieldIndexer = new FieldIndexer(globalDictionary);
          fieldIndexers.put(fieldName, fieldIndexer);
        }
        return fieldIndexer.processValue(eval);
      }
      return null;
    }
  };

  public AutoTypeColumnIndexer(String name, @Nullable ColumnType castToType)
  {
    this.columnName = name;
    if (castToType != null && (castToType.isPrimitive() || castToType.isPrimitiveArray())) {
      this.castToType = castToType;
      this.castToExpressionType = ExpressionType.fromColumnTypeStrict(castToType);
    } else {
      this.castToType = null;
      this.castToExpressionType = null;
    }
  }

  @Override
  public EncodedKeyComponent<StructuredData> processRowValsToUnsortedEncodedKeyComponent(
      @Nullable Object dimValues,
      boolean reportParseExceptions
  )
  {
    if (firstRow) {
      constantValue = dimValues;
      firstRow = false;
    } else if (isConstant) {
      isConstant = Objects.equals(dimValues, constantValue);
    }

    if (castToExpressionType != null) {
      return processCast(dimValues);
    } else {
      return processAuto(dimValues);
    }
  }

  /**
   * Process values which will all be cast to {@link #castToExpressionType}. This method should not be used for
   * and does not handle actual nested data structures, use {@link #processAuto(Object)} instead.
   */
  private EncodedKeyComponent<StructuredData> processCast(@Nullable Object dimValues)
  {
    final long oldDictSizeInBytes = globalDictionary.sizeInBytes();
    final int oldFieldKeySize = estimatedFieldKeySize;
    ExprEval<?> eval = ExprEval.bestEffortOf(dimValues);
    try {
      eval = eval.castTo(castToExpressionType);
    }
    catch (IAE invalidCast) {
      throw new ParseException(eval.asString(), invalidCast, "Cannot coerce column [%s] input to requested type [%s]", columnName, castToType);
    }

    FieldIndexer fieldIndexer = fieldIndexers.get(NestedPathFinder.JSON_PATH_ROOT);
    if (fieldIndexer == null) {
      estimatedFieldKeySize += StructuredDataProcessor.estimateStringSize(NestedPathFinder.JSON_PATH_ROOT);
      fieldIndexer = new FieldIndexer(globalDictionary);
      fieldIndexers.put(NestedPathFinder.JSON_PATH_ROOT, fieldIndexer);
    }
    StructuredDataProcessor.ProcessedValue<?> rootValue = fieldIndexer.processValue(eval);
    long effectiveSizeBytes = rootValue.getSize();
    // then, we add the delta of size change to the global dictionaries to account for any new space added by the
    // 'raw' data
    effectiveSizeBytes += (globalDictionary.sizeInBytes() - oldDictSizeInBytes);
    effectiveSizeBytes += (estimatedFieldKeySize - oldFieldKeySize);
    return new EncodedKeyComponent<>(StructuredData.wrap(eval.value()), effectiveSizeBytes);
  }

  /**
   * Process potentially nested data using {@link #indexerProcessor}, a {@link StructuredDataProcessor} which visits
   * all children to catalog values into the {@link #globalDictionary}, building {@link FieldIndexer} along the way
   * for each primitive or array primitive value encountered.
   */
  private EncodedKeyComponent<StructuredData> processAuto(@Nullable Object dimValues)
  {
    final long oldDictSizeInBytes = globalDictionary.sizeInBytes();
    final int oldFieldKeySize = estimatedFieldKeySize;
    final StructuredData data;
    if (dimValues == null) {
      hasNulls = true;
      data = null;
    } else if (dimValues instanceof StructuredData) {
      data = (StructuredData) dimValues;
    } else {
      data = new StructuredData(dimValues);
    }
    final StructuredDataProcessor.ProcessResults info = indexerProcessor.processFields(
        data == null ? null : data.getValue()
    );
    if (info.hasObjects()) {
      hasNestedData = true;
    }
    // 'raw' data is currently preserved 'as-is', and not replaced with object references to the global dictionaries
    long effectiveSizeBytes = info.getEstimatedSize();
    // then, we add the delta of size change to the global dictionaries to account for any new space added by the
    // 'raw' data
    effectiveSizeBytes += (globalDictionary.sizeInBytes() - oldDictSizeInBytes);
    effectiveSizeBytes += (estimatedFieldKeySize - oldFieldKeySize);
    return new EncodedKeyComponent<>(data, effectiveSizeBytes);
  }

  @Override
  public void setSparseIndexed()
  {
    this.hasNulls = true;
    if (firstRow) {
      firstRow = false;
    } else if (constantValue != null) {
      constantValue = null;
      isConstant = false;
    }
  }

  @Override
  public StructuredData getUnsortedEncodedValueFromSorted(StructuredData sortedIntermediateValue)
  {
    return sortedIntermediateValue;
  }

  @Override
  public CloseableIndexed<StructuredData> getSortedIndexedValues()
  {
    throw new UnsupportedOperationException("Not supported");
  }

  public SortedValueDictionary getSortedValueLookups()
  {
    return globalDictionary.getSortedCollector();
  }

  public SortedMap<String, FieldTypeInfo.MutableTypeSet> getFieldTypeInfo()
  {
    final TreeMap<String, FieldTypeInfo.MutableTypeSet> fields = new TreeMap<>();
    for (Map.Entry<String, FieldIndexer> entry : fieldIndexers.entrySet()) {
      // skip adding the field if no types are in the set, meaning only null values have been processed
      if (!entry.getValue().getTypes().isEmpty() || entry.getValue().getTypes().hasUntypedArray()) {
        fields.put(entry.getKey(), entry.getValue().getTypes());
      }
    }
    return fields;
  }

  @Override
  public StructuredData getMinValue()
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public StructuredData getMaxValue()
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public int getCardinality()
  {
    return globalDictionary.getCardinality();
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec spec,
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    if (fieldIndexers.size() == 0 && isConstant && !hasNestedData) {
      return DimensionSelector.constant(null, spec.getExtractionFn());
    }
    final ColumnValueSelector<?> rootLiteralSelector = getRootLiteralValueSelector(currEntry, dimIndex);
    if (rootLiteralSelector != null) {
      final FieldIndexer root = fieldIndexers.get(NestedPathFinder.JSON_PATH_ROOT);
      final ColumnType rootType = root.isSingleType() ? root.getTypes().getSingleType() : getLogicalType();
      if (rootType.isArray()) {
        throw new UOE(
            "makeDimensionSelector is not supported, column [%s] is [%s] typed and should only use makeColumnValueSelector",
            spec.getOutputName(),
            rootType
        );
      }
      if (spec.getExtractionFn() == null) {
        return new BaseSingleValueDimensionSelector()
        {
          @Nullable
          @Override
          protected String getValue()
          {
            return Evals.asString(rootLiteralSelector.getObject());
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {

          }
        };
      }
      return new BaseSingleValueDimensionSelector()
      {
        @Nullable
        @Override
        protected String getValue()
        {
          final String s = Evals.asString(rootLiteralSelector.getObject());
          return spec.getExtractionFn().apply(s);
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {

        }
      };
    }
    // column has nested data or is of mixed root type, cannot use
    throw new UOE(
        "makeDimensionSelector is not supported, column [%s] is [%s] typed and should only use makeColumnValueSelector",
        spec.getOutputName(),
        ColumnType.NESTED_DATA
    );
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    final ColumnValueSelector<?> rootLiteralSelector = getRootLiteralValueSelector(currEntry, dimIndex);
    if (rootLiteralSelector != null) {
      return rootLiteralSelector;
    }

    return new ObjectColumnSelector<StructuredData>()
    {
      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Nullable
      @Override
      public StructuredData getObject()
      {
        final Object[] dims = currEntry.get().getDims();
        if (0 <= dimIndex && dimIndex < dims.length) {
          return (StructuredData) dims[dimIndex];
        } else {
          return null;
        }
      }

      @Override
      public Class<StructuredData> classOfObject()
      {
        return StructuredData.class;
      }
    };
  }

  @Override
  public ColumnCapabilities getColumnCapabilities()
  {
    return ColumnCapabilitiesImpl.createDefault()
                                 .setType(getLogicalType())
                                 .setHasNulls(hasNulls);
  }

  public ColumnType getLogicalType()
  {
    if (castToType != null) {
      return castToType;
    }
    if (hasNestedData) {
      return ColumnType.NESTED_DATA;
    }
    if (isConstant && constantValue == null) {
      // we didn't see anything, so we can be anything, so why not a string?
      return ColumnType.STRING;
    }
    if (fieldIndexers.size() == 1 && fieldIndexers.containsKey(NestedPathFinder.JSON_PATH_ROOT)) {
      FieldIndexer rootField = fieldIndexers.get(NestedPathFinder.JSON_PATH_ROOT);
      ColumnType logicalType = null;
      for (ColumnType type : FieldTypeInfo.convertToSet(rootField.getTypes().getByteValue())) {
        logicalType = ColumnType.leastRestrictiveType(logicalType, type);
      }
      if (logicalType != null) {
        // special handle empty arrays
        if (!rootField.getTypes().hasUntypedArray() || logicalType.isArray()) {
          return logicalType;
        }
        return ColumnTypeFactory.getInstance().ofArray(logicalType);
      }
      // if we only have empty an null arrays, ARRAY<LONG> is the most restrictive type we can pick
      if (rootField.getTypes().hasUntypedArray()) {
        return ColumnType.LONG_ARRAY;
      }
    }
    return ColumnType.NESTED_DATA;
  }

  public boolean isConstant()
  {
    return isConstant;
  }

  @Nullable
  public Object getConstantValue()
  {
    return constantValue;
  }

  @Override
  public ColumnFormat getFormat()
  {
    return new Format(getLogicalType(), hasNulls, castToType != null);
  }

  @Override
  public int compareUnsortedEncodedKeyComponents(
      @Nullable StructuredData lhs,
      @Nullable StructuredData rhs
  )
  {
    return StructuredData.COMPARATOR.compare(lhs, rhs);
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(
      @Nullable StructuredData lhs,
      @Nullable StructuredData rhs
  )
  {
    return Objects.equals(lhs, rhs);
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(@Nullable StructuredData key)
  {
    return Objects.hash(key);
  }

  @Override
  public Object convertUnsortedEncodedKeyComponentToActualList(StructuredData key)
  {
    return key;
  }

  @Override
  public ColumnValueSelector convertUnsortedValuesToSorted(ColumnValueSelector selectorWithUnsortedValues)
  {
    final FieldIndexer rootIndexer = fieldIndexers.get(NestedPathFinder.JSON_PATH_ROOT);
    if (fieldIndexers.size() == 1 && rootIndexer != null && rootIndexer.isSingleType()) {
      // for root only literals, makeColumnValueSelector and makeDimensionSelector automatically unwrap StructuredData
      // we need to do the opposite here, wrapping selector values with a StructuredData so that they are consistently
      // typed for the merger
      return new ColumnValueSelector<StructuredData>()
      {
        @Override
        public boolean isNull()
        {
          return selectorWithUnsortedValues.isNull();
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          selectorWithUnsortedValues.inspectRuntimeShape(inspector);
        }

        @Nullable
        @Override
        public StructuredData getObject()
        {
          return StructuredData.wrap(selectorWithUnsortedValues.getObject());
        }

        @Override
        public float getFloat()
        {
          return selectorWithUnsortedValues.getFloat();
        }

        @Override
        public double getDouble()
        {
          return selectorWithUnsortedValues.getDouble();
        }

        @Override
        public long getLong()
        {
          return selectorWithUnsortedValues.getLong();
        }

        @Override
        public Class<StructuredData> classOfObject()
        {
          return StructuredData.class;
        }
      };
    }
    return selectorWithUnsortedValues;
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      StructuredData key,
      int rowNum,
      MutableBitmap[] bitmapIndexes,
      BitmapFactory factory
  )
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Nullable
  private ColumnValueSelector<?> getRootLiteralValueSelector(
      IncrementalIndexRowHolder currEntry,
      int dimIndex
  )
  {
    if (fieldIndexers.size() > 1 || hasNestedData) {
      return null;
    }
    final FieldIndexer root = fieldIndexers.get(NestedPathFinder.JSON_PATH_ROOT);
    if (root == null) {
      return null;
    }
    final Object defaultValue = getDefaultValueForType(getLogicalType());
    return new ColumnValueSelector<Object>()
    {
      @Override
      public boolean isNull()
      {
        final Object o = getObject();
        return computeNumber(o) == null;
      }

      @Override
      public float getFloat()
      {
        Number value = computeNumber(getObject());
        if (value == null) {
          return 0;
        }
        return value.floatValue();
      }

      @Override
      public double getDouble()
      {
        Number value = computeNumber(getObject());
        if (value == null) {
          return 0;
        }
        return value.doubleValue();
      }

      @Override
      public long getLong()
      {
        Number value = computeNumber(getObject());
        if (value == null) {
          return 0;
        }
        return value.longValue();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Nullable
      @Override
      public Object getObject()
      {
        final Object[] dims = currEntry.get().getDims();
        if (0 <= dimIndex && dimIndex < dims.length) {
          final StructuredData data = (StructuredData) dims[dimIndex];
          if (data != null) {
            final Object o = ExprEval.bestEffortOf(data.getValue()).valueOrDefault();
            return o == null ? defaultValue : o;
          }
        }

        return defaultValue;
      }

      @Nullable
      private Number computeNumber(@Nullable Object o)
      {
        if (o instanceof Number) {
          return (Number) o;
        }
        if (o instanceof String) {
          Long l = GuavaUtils.tryParseLong((String) o);
          if (l != null) {
            return l;
          }
          return Doubles.tryParse((String) o);
        }
        return null;
      }

      @Override
      public Class<?> classOfObject()
      {
        return Object.class;
      }
    };
  }

  static class FieldIndexer
  {
    private final ValueDictionary valueDictionary;
    private final FieldTypeInfo.MutableTypeSet typeSet;

    FieldIndexer(ValueDictionary valueDictionary)
    {
      this.valueDictionary = valueDictionary;
      this.typeSet = new FieldTypeInfo.MutableTypeSet();
    }

    private StructuredDataProcessor.ProcessedValue<?> processValue(ExprEval<?> eval)
    {
      final ExpressionType columnType = eval.type();
      int sizeEstimate;
      switch (columnType.getType()) {
        case LONG:
          typeSet.add(ColumnType.LONG);
          sizeEstimate = valueDictionary.addLongValue(eval.asLong());
          return new StructuredDataProcessor.ProcessedValue<>(eval.asLong(), sizeEstimate);
        case DOUBLE:
          typeSet.add(ColumnType.DOUBLE);
          sizeEstimate = valueDictionary.addDoubleValue(eval.asDouble());
          return new StructuredDataProcessor.ProcessedValue<>(eval.asDouble(), sizeEstimate);
        case ARRAY:
          // sanity check, this should never happen
          if (columnType.getElementType() == null) {
            throw new IAE(
                "Array type [%s] missing element type, how did this possibly happen?",
                eval.type()
            );
          }

          final Object[] theArray = eval.asArray();
          if (theArray == null) {
            typeSet.addUntypedArray();
          } else {
            switch (columnType.getElementType().getType()) {
              case LONG:
                typeSet.add(ColumnType.LONG_ARRAY);
                sizeEstimate = valueDictionary.addLongArray(theArray);
                return new StructuredDataProcessor.ProcessedValue<>(theArray, sizeEstimate);
              case DOUBLE:
                typeSet.add(ColumnType.DOUBLE_ARRAY);
                sizeEstimate = valueDictionary.addDoubleArray(theArray);
                return new StructuredDataProcessor.ProcessedValue<>(theArray, sizeEstimate);
              case STRING:
                // empty arrays and arrays with all nulls are detected as string arrays, but don't count them as part of
                // the type set yet, we'll handle that later when serializing
                if (theArray.length == 0 || Arrays.stream(theArray).allMatch(Objects::isNull)) {
                  typeSet.addUntypedArray();
                } else {
                  typeSet.add(ColumnType.STRING_ARRAY);
                }
                sizeEstimate = valueDictionary.addStringArray(theArray);
                return new StructuredDataProcessor.ProcessedValue<>(theArray, sizeEstimate);
              default:
                throw new IAE("Unhandled type: %s", columnType);
            }
          }
        case STRING:
          typeSet.add(ColumnType.STRING);
          final String asString = eval.asString();
          sizeEstimate = valueDictionary.addStringValue(asString);
          return new StructuredDataProcessor.ProcessedValue<>(asString, sizeEstimate);
        default:
          throw new IAE("Unhandled type: %s", columnType);
      }
    }

    public FieldTypeInfo.MutableTypeSet getTypes()
    {
      return typeSet;
    }

    public boolean isSingleType()
    {
      return typeSet.getSingleType() != null;
    }
  }

  static class Format implements ColumnFormat
  {
    private final ColumnType logicalType;
    private final boolean hasNulls;
    private final boolean enforceLogicalType;

    Format(ColumnType logicalType, boolean hasNulls, boolean enforceLogicalType)
    {
      this.logicalType = logicalType;
      this.hasNulls = hasNulls;
      this.enforceLogicalType = enforceLogicalType;
    }

    @Override
    public ColumnType getLogicalType()
    {
      return logicalType;
    }

    @Override
    public DimensionHandler getColumnHandler(String columnName)
    {
      return new NestedCommonFormatColumnHandler(columnName, enforceLogicalType ? logicalType : null);
    }

    @Override
    public DimensionSchema getColumnSchema(String columnName)
    {
      return new AutoTypeColumnSchema(columnName, enforceLogicalType ? logicalType : null);
    }

    @Override
    public ColumnFormat merge(@Nullable ColumnFormat otherFormat)
    {
      if (otherFormat == null) {
        return this;
      }
      if (otherFormat instanceof Format) {
        final Format other = (Format) otherFormat;
        if (!getLogicalType().equals(other.getLogicalType())) {
          return new Format(ColumnType.NESTED_DATA, hasNulls || other.hasNulls, false);
        }
        return new Format(logicalType, hasNulls || other.hasNulls, enforceLogicalType || other.enforceLogicalType);
      }
      throw new ISE(
          "Cannot merge columns of type[%s] and format[%s] and with [%s] and [%s]",
          logicalType,
          this.getClass().getName(),
          otherFormat.getLogicalType(),
          otherFormat.getClass().getName()
      );
    }

    @Override
    public ColumnCapabilities toColumnCapabilities()
    {
      return ColumnCapabilitiesImpl.createDefault()
                                   .setType(logicalType)
                                   .setHasNulls(hasNulls);
    }
  }

  @Nullable
  private static Object getDefaultValueForType(@Nullable ColumnType columnType)
  {
    if (NullHandling.replaceWithDefault()) {
      if (columnType != null) {
        if (ColumnType.LONG.equals(columnType)) {
          return NullHandling.defaultLongValue();
        } else if (ColumnType.DOUBLE.equals(columnType)) {
          return NullHandling.defaultDoubleValue();
        }
      }
    }
    return null;
  }
}
