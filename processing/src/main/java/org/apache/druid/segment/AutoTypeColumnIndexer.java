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

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnType;
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
  protected volatile boolean hasNulls = false;

  protected SortedMap<String, FieldIndexer> fieldIndexers = new TreeMap<>();
  protected final ValueDictionary globalDictionary = new ValueDictionary();

  int estimatedFieldKeySize = 0;

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
      if (eval.type().isArray() && eval.type().getElementType().isPrimitive()) {
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

  @Override
  public EncodedKeyComponent<StructuredData> processRowValsToUnsortedEncodedKeyComponent(
      @Nullable Object dimValues,
      boolean reportParseExceptions
  )
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
    StructuredDataProcessor.ProcessResults info = indexerProcessor.processFields(data == null ? null : data.getValue());
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
      if (!entry.getValue().getTypes().isEmpty()) {
        fields.put(entry.getKey(), entry.getValue().getTypes());
      }
    }
    // special handling for when column only has arrays with null elements, treat it as a string array
    if (fields.isEmpty() && fieldIndexers.size() == 1) {
      fields.put(fieldIndexers.firstKey(), new FieldTypeInfo.MutableTypeSet().add(ColumnType.STRING_ARRAY));
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
    final ColumnValueSelector<?> rootLiteralSelector = getRootLiteralValueSelector(currEntry, dimIndex);
    if (rootLiteralSelector != null) {
      final FieldIndexer root = fieldIndexers.get(NestedPathFinder.JSON_PATH_ROOT);
      final ColumnType rootType = root.getTypes().getSingleType();
      if (rootType.isArray()) {
        throw new UOE(
            "makeDimensionSelector is not supported, column [%s] is [%s] typed and should only use makeColumnValueSelector",
            spec.getOutputName(),
            rootType
        );
      }
      return new BaseSingleValueDimensionSelector()
      {
        @Nullable
        @Override
        protected String getValue()
        {
          final Object o = rootLiteralSelector.getObject();
          if (o == null) {
            return null;
          }
          return o.toString();
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

  private ColumnType getLogicalType()
  {
    if (fieldIndexers.isEmpty()) {
      // we didn't see anything, so we can be anything, so why not a string?
      return ColumnType.STRING;
    }
    if (fieldIndexers.size() == 1 && fieldIndexers.containsKey(NestedPathFinder.JSON_PATH_ROOT)) {
      FieldIndexer rootField = fieldIndexers.get(NestedPathFinder.JSON_PATH_ROOT);
      ColumnType singleType = rootField.getTypes().getSingleType();
      return singleType == null ? ColumnType.NESTED_DATA : singleType;
    }
    return ColumnType.NESTED_DATA;
  }

  @Override
  public ColumnFormat getFormat()
  {
    return new Format(getLogicalType(), hasNulls);
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
    if (fieldIndexers.size() > 1) {
      return null;
    }
    final FieldIndexer root = fieldIndexers.get(NestedPathFinder.JSON_PATH_ROOT);
    if (root == null || !root.isSingleType()) {
      return null;
    }
    final Object defaultValue = getDefaultValueForType(root.getTypes().getSingleType());
    return new ColumnValueSelector<Object>()
    {
      @Override
      public boolean isNull()
      {
        final Object o = getObject();
        return !(o instanceof Number);
      }

      @Override
      public float getFloat()
      {
        Object value = getObject();
        if (value == null) {
          return 0;
        }
        return ((Number) value).floatValue();
      }

      @Override
      public double getDouble()
      {
        Object value = getObject();
        if (value == null) {
          return 0;
        }
        return ((Number) value).doubleValue();
      }

      @Override
      public long getLong()
      {
        Object value = getObject();
        if (value == null) {
          return 0;
        }
        return ((Number) value).longValue();
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
          switch (columnType.getElementType().getType()) {
            case LONG:
              typeSet.add(ColumnType.LONG_ARRAY);
              final Object[] longArray = eval.asArray();
              sizeEstimate = valueDictionary.addLongArray(longArray);
              return new StructuredDataProcessor.ProcessedValue<>(longArray, sizeEstimate);
            case DOUBLE:
              typeSet.add(ColumnType.DOUBLE_ARRAY);
              final Object[] doubleArray = eval.asArray();
              sizeEstimate = valueDictionary.addDoubleArray(doubleArray);
              return new StructuredDataProcessor.ProcessedValue<>(doubleArray, sizeEstimate);
            case STRING:
              final Object[] stringArray = eval.asArray();
              // empty arrays and arrays with all nulls are detected as string arrays, but dont count them as part of
              // the type set
              if (stringArray.length > 0 && !Arrays.stream(stringArray).allMatch(Objects::isNull)) {
                typeSet.add(ColumnType.STRING_ARRAY);
              }
              sizeEstimate = valueDictionary.addStringArray(stringArray);
              return new StructuredDataProcessor.ProcessedValue<>(stringArray, sizeEstimate);
            default:
              throw new IAE("Unhandled type: %s", columnType);
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

    Format(ColumnType logicalType, boolean hasNulls)
    {
      this.logicalType = logicalType;
      this.hasNulls = hasNulls;
    }

    @Override
    public ColumnType getLogicalType()
    {
      return logicalType;
    }

    @Override
    public DimensionHandler getColumnHandler(String columnName)
    {
      return new NestedCommonFormatColumnHandler(columnName);
    }

    @Override
    public DimensionSchema getColumnSchema(String columnName)
    {
      return new AutoTypeColumnSchema(columnName);
    }

    @Override
    public ColumnFormat merge(@Nullable ColumnFormat otherFormat)
    {
      if (otherFormat == null) {
        return this;
      }
      if (otherFormat instanceof Format) {
        final boolean otherHasNulls = ((Format) otherFormat).hasNulls;
        if (!getLogicalType().equals(otherFormat.getLogicalType())) {
          return new Format(ColumnType.NESTED_DATA, hasNulls || otherHasNulls);
        }
        return new Format(logicalType, hasNulls || otherHasNulls);
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
