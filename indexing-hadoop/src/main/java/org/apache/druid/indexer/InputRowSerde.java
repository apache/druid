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

package org.apache.druid.indexer;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Rows;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.hadoop.io.WritableUtils;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 */
public class InputRowSerde
{
  private static final Logger log = new Logger(InputRowSerde.class);

  private static final IndexSerdeTypeHelper STRING_HELPER = new StringIndexSerdeTypeHelper();
  private static final IndexSerdeTypeHelper LONG_HELPER = new LongIndexSerdeTypeHelper();
  private static final IndexSerdeTypeHelper FLOAT_HELPER = new FloatIndexSerdeTypeHelper();
  private static final IndexSerdeTypeHelper DOUBLE_HELPER = new DoubleIndexSerdeTypeHelper();

  private static <T extends Number> void writeNullableNumeric(
      T ret,
      final ByteArrayDataOutput out,
      final Supplier<T> getDefault,
      final Consumer<T> write)
  {
    if (ret == null) {
      ret = getDefault.get();
    }

    // Write the null byte only if the default numeric value is still null.
    if (ret == null) {
      out.writeByte(NullHandling.IS_NULL_BYTE);
      return;
    }

    if (NullHandling.sqlCompatible()) {
      out.writeByte(NullHandling.IS_NOT_NULL_BYTE);
    }

    write.accept(ret);
  }

  private static boolean isNullByteSet(final ByteArrayDataInput in)
  {
    return NullHandling.sqlCompatible() && in.readByte() == NullHandling.IS_NULL_BYTE;
  }

  public interface IndexSerdeTypeHelper<T>
  {
    ValueType getType();

    void serialize(ByteArrayDataOutput out, Object value);

    T deserialize(ByteArrayDataInput in);
  }

  public static Map<String, IndexSerdeTypeHelper> getTypeHelperMap(DimensionsSpec dimensionsSpec)
  {
    Map<String, IndexSerdeTypeHelper> typeHelperMap = new HashMap<>();
    for (DimensionSchema dimensionSchema : dimensionsSpec.getDimensions()) {
      IndexSerdeTypeHelper typeHelper;
      switch (dimensionSchema.getColumnType().getType()) {
        case STRING:
          typeHelper = STRING_HELPER;
          break;
        case LONG:
          typeHelper = LONG_HELPER;
          break;
        case FLOAT:
          typeHelper = FLOAT_HELPER;
          break;
        case DOUBLE:
          typeHelper = DOUBLE_HELPER;
          break;
        default:
          throw new IAE("Invalid type: [%s]", dimensionSchema.getColumnType());
      }
      typeHelperMap.put(dimensionSchema.getName(), typeHelper);
    }
    return typeHelperMap;
  }

  public static class SerializeResult
  {
    private final byte[] serializedRow;
    private final List<String> parseExceptionMessages;

    public SerializeResult(
        final byte[] serializedRow,
        final List<String> parseExceptionMessages
    )
    {
      this.serializedRow = serializedRow;
      this.parseExceptionMessages = parseExceptionMessages;
    }

    public byte[] getSerializedRow()
    {
      return serializedRow;
    }

    public List<String> getParseExceptionMessages()
    {
      return parseExceptionMessages;
    }
  }

  public static class StringIndexSerdeTypeHelper implements IndexSerdeTypeHelper<List<String>>
  {
    @Override
    public ValueType getType()
    {
      return ValueType.STRING;
    }

    @Override
    public void serialize(ByteArrayDataOutput out, Object value)
    {
      List<String> values = Rows.objectToStrings(value);
      try {
        writeStringArray(values, out);
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public List<String> deserialize(ByteArrayDataInput in)
    {
      try {
        return readStringArray(in);
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  public static class LongIndexSerdeTypeHelper implements IndexSerdeTypeHelper<Long>
  {
    @Override
    public ValueType getType()
    {
      return ValueType.LONG;
    }

    @Override
    public void serialize(ByteArrayDataOutput out, Object value)
    {
      ParseException exceptionToThrow = null;
      Long ret = null;
      try {
        ret = DimensionHandlerUtils.convertObjectToLong(value, true);
      }
      catch (ParseException pe) {
        exceptionToThrow = pe;
      }

      writeNullableNumeric(ret, out, NullHandling::defaultLongValue, out::writeLong);

      if (exceptionToThrow != null) {
        throw exceptionToThrow;
      }
    }

    @Override
    @Nullable
    public Long deserialize(ByteArrayDataInput in)
    {
      return isNullByteSet(in) ? null : in.readLong();
    }
  }

  public static class FloatIndexSerdeTypeHelper implements IndexSerdeTypeHelper<Float>
  {
    @Override
    public ValueType getType()
    {
      return ValueType.FLOAT;
    }

    @Override
    public void serialize(ByteArrayDataOutput out, Object value)
    {
      ParseException exceptionToThrow = null;
      Float ret = null;
      try {
        ret = DimensionHandlerUtils.convertObjectToFloat(value, true);
      }
      catch (ParseException pe) {
        exceptionToThrow = pe;
      }

      writeNullableNumeric(ret, out, NullHandling::defaultFloatValue, out::writeFloat);

      if (exceptionToThrow != null) {
        throw exceptionToThrow;
      }
    }

    @Override
    @Nullable
    public Float deserialize(ByteArrayDataInput in)
    {
      return isNullByteSet(in) ? null : in.readFloat();
    }
  }

  public static class DoubleIndexSerdeTypeHelper implements IndexSerdeTypeHelper<Double>
  {
    @Override
    public ValueType getType()
    {
      return ValueType.DOUBLE;
    }

    @Override
    public void serialize(ByteArrayDataOutput out, Object value)
    {
      ParseException exceptionToThrow = null;
      Double ret = null;
      try {
        ret = DimensionHandlerUtils.convertObjectToDouble(value, true);
      }
      catch (ParseException pe) {
        exceptionToThrow = pe;
      }

      writeNullableNumeric(ret, out, NullHandling::defaultDoubleValue, out::writeDouble);

      if (exceptionToThrow != null) {
        throw exceptionToThrow;
      }
    }

    @Override
    @Nullable
    public Double deserialize(ByteArrayDataInput in)
    {
      return isNullByteSet(in) ? null : in.readDouble();
    }
  }

  public static SerializeResult toBytes(
      final Map<String, IndexSerdeTypeHelper> typeHelperMap,
      final InputRow row,
      final AggregatorFactory[] aggs
  )
  {
    try {
      List<String> parseExceptionMessages = new ArrayList<>();
      ByteArrayDataOutput out = ByteStreams.newDataOutput();

      //write timestamp
      out.writeLong(row.getTimestampFromEpoch());

      //writing all dimensions
      List<String> dimList = row.getDimensions();

      WritableUtils.writeVInt(out, dimList.size());
      for (String dim : dimList) {
        IndexSerdeTypeHelper typeHelper = typeHelperMap.get(dim);
        if (typeHelper == null) {
          typeHelper = STRING_HELPER;
        }
        writeString(dim, out);

        try {
          typeHelper.serialize(out, row.getRaw(dim));
        }
        catch (ParseException pe) {
          parseExceptionMessages.add(pe.getMessage());
        }
      }

      //writing all metrics
      WritableUtils.writeVInt(out, aggs.length);
      for (AggregatorFactory aggFactory : aggs) {
        String k = aggFactory.getName();
        writeString(k, out);

        final IncrementalIndex.InputRowHolder holder = new IncrementalIndex.InputRowHolder();
        holder.set(row);
        try (Aggregator agg = aggFactory.factorize(
            IncrementalIndex.makeColumnSelectorFactory(VirtualColumns.EMPTY, holder, aggFactory)
        )) {
          try {
            agg.aggregate();
          }
          catch (ParseException e) {
            // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
            log.debug(e, "Encountered parse error, skipping aggregator[%s].", k);
            parseExceptionMessages.add(e.getMessage());
          }

          final ColumnType type = aggFactory.getIntermediateType();

          if (agg.isNull()) {
            out.writeByte(NullHandling.IS_NULL_BYTE);
          } else {
            out.writeByte(NullHandling.IS_NOT_NULL_BYTE);
            if (type.is(ValueType.FLOAT)) {
              out.writeFloat(agg.getFloat());
            } else if (type.is(ValueType.LONG)) {
              WritableUtils.writeVLong(out, agg.getLong());
            } else if (type.is(ValueType.DOUBLE)) {
              out.writeDouble(agg.getDouble());
            } else if (type.is(ValueType.COMPLEX)) {
              Object val = agg.get();
              ComplexMetricSerde serde = getComplexMetricSerde(type.getComplexTypeName());
              writeBytes(serde.toBytes(val), out);
            } else {
              throw new IAE("Unable to serialize type[%s]", type.asTypeString());
            }
          }
        }
      }

      return new SerializeResult(out.toByteArray(), parseExceptionMessages);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static void writeBytes(@Nullable byte[] value, ByteArrayDataOutput out) throws IOException
  {
    int length = value == null ? -1 : value.length;
    WritableUtils.writeVInt(out, length);
    if (value != null) {
      out.write(value, 0, value.length);
    }
  }

  private static void writeString(String value, ByteArrayDataOutput out) throws IOException
  {
    writeBytes(StringUtils.toUtf8(value), out);
  }

  private static void writeStringArray(List<String> values, ByteArrayDataOutput out) throws IOException
  {
    if (values == null || values.size() == 0) {
      WritableUtils.writeVInt(out, 0);
      return;
    }
    WritableUtils.writeVInt(out, values.size());
    for (String value : values) {
      writeString(value, out);
    }
  }

  private static String readString(DataInput in) throws IOException
  {
    byte[] result = readBytes(in);
    return StringUtils.fromUtf8(result);
  }

  private static byte[] readBytes(DataInput in) throws IOException
  {
    int size = WritableUtils.readVInt(in);
    byte[] result = new byte[size];
    in.readFully(result, 0, size);
    return result;
  }

  @Nullable
  private static List<String> readStringArray(DataInput in) throws IOException
  {
    int count = WritableUtils.readVInt(in);
    if (count == 0) {
      return null;
    }
    List<String> values = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      values.add(readString(in));
    }
    return values;
  }

  public static InputRow fromBytes(
      final Map<String, IndexSerdeTypeHelper> typeHelperMap,
      byte[] data,
      AggregatorFactory[] aggs
  )
  {
    try {
      ByteArrayDataInput in = ByteStreams.newDataInput(data);

      //Read timestamp
      long timestamp = in.readLong();

      Map<String, Object> event = new HashMap<>();

      //Read dimensions
      List<String> dimensions = new ArrayList<>();
      int dimNum = WritableUtils.readVInt(in);
      for (int i = 0; i < dimNum; i++) {
        String dimension = readString(in);
        dimensions.add(dimension);

        IndexSerdeTypeHelper typeHelper = typeHelperMap.get(dimension);
        if (typeHelper == null) {
          typeHelper = STRING_HELPER;
        }
        Object dimValues = typeHelper.deserialize(in);
        if (dimValues == null) {
          continue;
        }

        if (typeHelper.getType() == ValueType.STRING) {
          List<String> dimensionValues = (List<String>) dimValues;
          if (dimensionValues.size() == 1) {
            event.put(dimension, dimensionValues.get(0));
          } else {
            event.put(dimension, dimensionValues);
          }
        } else {
          event.put(dimension, dimValues);
        }
      }

      //Read metrics
      int metricSize = WritableUtils.readVInt(in);
      for (int i = 0; i < metricSize; i++) {
        final String metric = readString(in);
        final AggregatorFactory agg = getAggregator(metric, aggs, i);
        final ColumnType type = agg.getIntermediateType();
        final byte metricNullability = in.readByte();

        if (metricNullability == NullHandling.IS_NULL_BYTE) {
          // metric value is null.
          continue;
        }
        if (type.is(ValueType.FLOAT)) {
          event.put(metric, in.readFloat());
        } else if (type.is(ValueType.LONG)) {
          event.put(metric, WritableUtils.readVLong(in));
        } else if (type.is(ValueType.DOUBLE)) {
          event.put(metric, in.readDouble());
        } else {
          ComplexMetricSerde serde = getComplexMetricSerde(agg.getIntermediateType().getComplexTypeName());
          byte[] value = readBytes(in);
          event.put(metric, serde.fromBytes(value, 0, value.length));
        }
      }

      return new MapBasedInputRow(timestamp, dimensions, event);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Nullable
  private static AggregatorFactory getAggregator(String metric, AggregatorFactory[] aggs, int i)
  {
    if (aggs[i].getName().equals(metric)) {
      return aggs[i];
    }
    log.warn("Aggs disordered, fall backs to loop.");
    for (AggregatorFactory agg : aggs) {
      if (agg.getName().equals(metric)) {
        return agg;
      }
    }
    return null;
  }

  private static ComplexMetricSerde getComplexMetricSerde(String type)
  {
    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(type);
    if (serde == null) {
      throw new IAE("Unknown type[%s]", type);
    }
    return serde;
  }
}
