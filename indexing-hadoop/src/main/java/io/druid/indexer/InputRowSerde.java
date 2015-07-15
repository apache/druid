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
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class InputRowSerde
{
  private static final Logger log = new Logger(InputRowSerde.class);

  private static final Text[] EMPTY_TEXT_ARRAY = new Text[0];

  public static final byte[] toBytes(final InputRow row, AggregatorFactory[] aggs)
  {
    try {
      ByteArrayDataOutput out = ByteStreams.newDataOutput();

      //write timestamp
      out.writeLong(row.getTimestampFromEpoch());

      //writing all dimensions
      List<String> dimList = row.getDimensions();

      Text[] dims = EMPTY_TEXT_ARRAY;
      if(dimList != null) {
        dims = new Text[dimList.size()];
        for (int i = 0; i < dims.length; i++) {
          dims[i] = new Text(dimList.get(i));
        }
      }
      StringArrayWritable sw = new StringArrayWritable(dims);
      sw.write(out);

      MapWritable mw = new MapWritable();

      if(dimList != null) {
        for (String dim : dimList) {
          List<String> dimValue = row.getDimension(dim);

          if (dimValue == null || dimValue.size() == 0) {
            continue;
          }

          if (dimValue.size() == 1) {
            mw.put(new Text(dim), new Text(dimValue.get(0)));
          } else {
            Text[] dimValueArr = new Text[dimValue.size()];
            for (int i = 0; i < dimValueArr.length; i++) {
              dimValueArr[i] = new Text(dimValue.get(i));
            }
            mw.put(new Text(dim), new StringArrayWritable(dimValueArr));
          }
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
      for (AggregatorFactory aggFactory : aggs) {
        String k = aggFactory.getName();

        Aggregator agg = aggFactory.factorize(
            IncrementalIndex.makeColumnSelectorFactory(
                aggFactory,
                supplier,
                true
            )
        );
        agg.aggregate();

        String t = aggFactory.getTypeName();

        if (t.equals("float")) {
          mw.put(new Text(k), new FloatWritable(agg.getFloat()));
        } else if (t.equals("long")) {
          mw.put(new Text(k), new LongWritable(agg.getLong()));
        } else {
          //its a complex metric
          Object val = agg.get();
          ComplexMetricSerde serde = getComplexMetricSerde(t);
          mw.put(new Text(k), new BytesWritable(serde.toBytes(val)));
        }
      }

      mw.write(out);
      return out.toByteArray();
    } catch(IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  public static final InputRow fromBytes(byte[] data, AggregatorFactory[] aggs)
  {
    try {
      DataInput in = ByteStreams.newDataInput(data);

      //Read timestamp
      long timestamp = in.readLong();

      //Read dimensions
      StringArrayWritable sw = new StringArrayWritable();
      sw.readFields(in);
      List<String> dimensions = Arrays.asList(sw.toStrings());

      MapWritable mw = new MapWritable();
      mw.readFields(in);

      Map<String, Object> event = Maps.newHashMap();

      for (String d : dimensions) {
        Writable v = mw.get(new Text(d));

        if (v == null) {
          continue;
        }

        if (v instanceof Text) {
          event.put(d, ((Text) v).toString());
        } else if (v instanceof StringArrayWritable) {
          event.put(d, Arrays.asList(((StringArrayWritable) v).toStrings()));
        } else {
          throw new ISE("unknown dim value type %s", v.getClass().getName());
        }
      }

      //Read metrics
      for (AggregatorFactory aggFactory : aggs) {
        String k = aggFactory.getName();
        Writable v = mw.get(new Text(k));

        if (v == null) {
          continue;
        }

        String t = aggFactory.getTypeName();

        if (t.equals("float")) {
          event.put(k, ((FloatWritable) v).get());
        } else if (t.equals("long")) {
          event.put(k, ((LongWritable) v).get());
        } else {
          //its a complex metric
          ComplexMetricSerde serde = getComplexMetricSerde(t);
          BytesWritable bw = (BytesWritable) v;
          event.put(k, serde.fromBytes(bw.getBytes(), 0, bw.getLength()));
        }
      }

      return new MapBasedInputRow(timestamp, dimensions, event);
    } catch(IOException ex) {
      throw Throwables.propagate(ex);
    }
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
