/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.VirtualColumns;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 */
public class InputRowSerde
{
  private static final Logger log = new Logger(InputRowSerde.class);

  public static final byte[] toBytes(final InputRow row, AggregatorFactory[] aggs, boolean reportParseExceptions)
  {
    try {
      ByteArrayDataOutput out = ByteStreams.newDataOutput();

      //write timestamp
      out.writeLong(row.getTimestampFromEpoch());

      //writing all dimensions
      List<String> dimList = row.getDimensions();

      WritableUtils.writeVInt(out, dimList.size());
      if (dimList != null) {
        for (String dim : dimList) {
          List<String> dimValues = row.getDimension(dim);
          writeString(dim, out);
          writeStringArray(dimValues, out);
        }
      }

      //writing all metrics
      Supplier<InputRow> supplier = new Supplier<InputRow>()
      {
        @Override
        public InputRow get()
        {
          return row;
        }
      };
      WritableUtils.writeVInt(out, aggs.length);
      for (AggregatorFactory aggFactory : aggs) {
        String k = aggFactory.getName();
        writeString(k, out);

        try (Aggregator agg = aggFactory.factorize(
            IncrementalIndex.makeColumnSelectorFactory(
                VirtualColumns.EMPTY,
                aggFactory,
                supplier,
                true
            )
        )) {
          try {
            agg.aggregate();
          }
          catch (ParseException e) {
            // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
            if (reportParseExceptions) {
              throw new ParseException(e, "Encountered parse error for aggregator[%s]", k);
            }
            log.debug(e, "Encountered parse error, skipping aggregator[%s].", k);
          }

          String t = aggFactory.getTypeName();

          if (t.equals("float")) {
            out.writeFloat(agg.getFloat());
          } else if (t.equals("long")) {
            WritableUtils.writeVLong(out, agg.getLong());
          } else if (t.equals("double")) {
            out.writeDouble(agg.getDouble());
          } else {
            //its a complex metric
            Object val = agg.get();
            ComplexMetricSerde serde = getComplexMetricSerde(t);
            writeBytes(serde.toBytes(val), out);
          }
        }
      }

      return out.toByteArray();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static void writeBytes(byte[] value, ByteArrayDataOutput out) throws IOException
  {
    WritableUtils.writeVInt(out, value.length);
    out.write(value, 0, value.length);
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

  public static final InputRow fromBytes(byte[] data, AggregatorFactory[] aggs)
  {
    try {
      DataInput in = ByteStreams.newDataInput(data);

      //Read timestamp
      long timestamp = in.readLong();

      Map<String, Object> event = Maps.newHashMap();

      //Read dimensions
      List<String> dimensions = Lists.newArrayList();
      int dimNum = WritableUtils.readVInt(in);
      for (int i = 0; i < dimNum; i++) {
        String dimension = readString(in);
        dimensions.add(dimension);
        List<String> dimensionValues = readStringArray(in);
        if (dimensionValues == null) {
          continue;
        }
        if (dimensionValues.size() == 1) {
          event.put(dimension, dimensionValues.get(0));
        } else {
          event.put(dimension, dimensionValues);
        }
      }

      //Read metrics
      int metricSize = WritableUtils.readVInt(in);
      for (int i = 0; i < metricSize; i++) {
        String metric = readString(in);
        String type = getType(metric, aggs, i);
        if (type.equals("float")) {
          event.put(metric, in.readFloat());
        } else if (type.equals("long")) {
          event.put(metric, WritableUtils.readVLong(in));
        } else if (type.equals("double")) {
          event.put(metric, in.readDouble());
        } else {
          ComplexMetricSerde serde = getComplexMetricSerde(type);
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

  private static String getType(String metric, AggregatorFactory[] aggs, int i)
  {
    if (aggs[i].getName().equals(metric)) {
      return aggs[i].getTypeName();
    }
    log.warn("Aggs disordered, fall backs to loop.");
    for (AggregatorFactory agg : aggs) {
      if (agg.getName().equals(metric)) {
        return agg.getTypeName();
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

class StringArrayWritable extends ArrayWritable
{
  public StringArrayWritable()
  {
    super(Text.class);
  }

  public StringArrayWritable(Text[] strs)
  {
    super(Text.class, strs);
  }
}
