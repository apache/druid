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

package org.apache.druid.spectator.histogram;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.netflix.spectator.api.histogram.PercentileBuckets;
import it.unimi.dsi.fastutil.shorts.Short2LongMap;
import it.unimi.dsi.fastutil.shorts.Short2LongMaps;
import it.unimi.dsi.fastutil.shorts.Short2LongOpenHashMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

// Since queries don't come from SpectatorHistogramAggregator in the case of
// using longSum or doubleSum aggregations. They come from LongSumBufferAggregator.
// Therefore, we extended Number here.
// This will prevent class casting exceptions if trying to query with sum rather
// than explicitly as a SpectatorHistogram
//
// The SpectatorHistogram is a Number. That number is of intValue(),
// which is the count of the number of events in the histogram
// (adding up the counts across all buckets).
//
// There are a few useful aggregators, which as Druid Native Queries use:
// type: "longSum" - Aggregates and returns the number of events in the histogram.
// i.e. the sum of all bucket counts.
// type: "spectatorHistogramDistribution" - Aggregates and returns a map (bucketIndex -> bucketCount)
// representing a SpectatorHistogram. The represented data is a distribution.
// type: "spectatorHistogramTimer" - Aggregates and returns a map (bucketIndex -> bucketCount)
// representing a SpectatorHistogram. The represented data is measuring time.
public class SpectatorHistogram extends Number
{
  private static final int MAX_ENTRY_BYTES = Short.BYTES + Long.BYTES;
  private static final int LOW_COUNT_FLAG = 0x0200;
  private static final int BYTE_VALUE = 0x8000;
  private static final int SHORT_VALUE = 0x4000;
  private static final int INT_VALUE = 0xC000;
  private static final int VALUE_SIZE_MASK = 0xFC00;
  private static final int KEY_MASK = 0x01FF;

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  // Values are packed into few bytes depending on the size of the counts
  // The bucket index falls in the range 0-276, so we need 9 bits for the bucket index.
  // Counts can range from 1 to Long.MAX_VALUE, so we need 1 to 64 bits for the value.
  // To optimize storage, we use the remaining top 7 bits of the bucket index short to
  // encode the storage type for the count value.
  // AAbb bbYx xxxx xxxx
  //        |          +-- 9 bits - The bucket index
  //        +------------- 1 bit  - Low-count flag, set if count <= 63
  // ++++ ++-------------- 6 bits - If low-count flag is set,
  //                                 The count value, zero extra bytes used.
  //                                If low-count flag is not set,
  //                                 The value length indicator as encoded below
  // ++------------------- 2 bits - 00 = 8 bytes used for value
  //                                10 = 1 byte used for value
  //                                01 = 2 bytes used for value
  //                                11 = 4 bytes used for value
  //
  // Example:
  // ------------------------------------------------------------------------------------------
  // Consider the histogram: [10, 30, 40x3, 50x2, 100x256]
  // That is there is one value of 10, and 3 values of 40, etc. As shown in the table below:
  //
  // Bucket Index | Bucket Range | Bucket Count
  //    10        |   [10,11)    |     1
  //    17        |   [26,31)    |     1
  //    19        |   [36,41)    |     3
  //    21        |   [46,51)    |     2
  //    25        |  [85,106)    |   256
  //
  // See com.netflix.spectator.api.histogram.PercentileBuckets
  // for an explaination of how the bucket index is assigned
  // to each of the values: (10, 17, 19, 21, 25).
  //
  // Based on the specification above the histogram is serialized into a
  // byte array to minimize storage size:
  // In Base 10: [64, 25, 1, 0, 6, 10, 6, 17, 14, 19, 10, 21]
  // In Binary: [01000000, 00011001, 00000001, 00000000, 00000110, 00001010,
  //             00000110, 00010001, 00001110, 00010011, 00001010, 00010101]
  //
  // Each groups of bits (which varies in length), represent a histogram bucket index and count
  // 01000000000110010000000100000000
  // 01 - Since the low count bit is NOT set, leading 2 bits 01 indicates that the bucket count
  //      value is encoded in 2 bytes.
  // 0000 - Since the low count bit is Not set these bits are unused, the bucket count will
  //        be encoded in an additional two bytes.
  // 0 - Low count bit is NOT set
  // 000011001 - These 9 bits represent the bucket index of 25
  // 0000000100000000 - These 16 bits represent the bucket count of 256
  //
  // 0000011000001010
  // 000001 - Low count bit IS set, so these 6-bits represent a bucket count of 1
  // 1 - Low count bit IS set
  // 000001010 - These 9 bits represent the bucket index of 10
  //
  // 0000011000010001
  // 000001 - Bucket count of 1
  // 1 - Low count bit IS set
  // 000010001 - Bucket index of 17
  //
  // 0000111000010011
  // 000011 - Bucket count of 3
  // 1 - Low count bit IS set
  // 000010011 - Bucket index of 19
  //
  // 0000101000010101
  // 000010 - Bucket count of 2
  // 1 - Low count bit IS set
  // 000010101 - Bucket index of 21
  // ------------------------------------------------------------------------------------------
  private Short2LongOpenHashMap backingMap;

  // The sum of counts in the histogram.
  // These are accumulated when an entry is added, or when another histogram is merged into this one.
  private long sumOfCounts = 0;

  static int getMaxIntermdiateHistogramSize()
  {
    return PercentileBuckets.length() * MAX_ENTRY_BYTES;
  }

  @Nullable
  static SpectatorHistogram deserialize(Object serializedHistogram)
  {
    if (serializedHistogram == null) {
      return null;
    }
    if (serializedHistogram instanceof byte[]) {
      return fromByteBuffer(ByteBuffer.wrap((byte[]) serializedHistogram));
    }
    if (serializedHistogram instanceof SpectatorHistogram) {
      return (SpectatorHistogram) serializedHistogram;
    }
    if (serializedHistogram instanceof String) {
      // Try parse as JSON into HashMap
      try {
        HashMap<String, Long> map = JSON_MAPPER.readerFor(HashMap.class).readValue((String) serializedHistogram);
        SpectatorHistogram histogram = new SpectatorHistogram();
        for (Map.Entry<String, Long> entry : map.entrySet()) {
          histogram.add(entry.getKey(), entry.getValue());
        }
        return histogram;
      }
      catch (JsonProcessingException e) {
        throw new ParseException((String) serializedHistogram, e, "String cannot be deserialized as JSON to a Spectator Histogram");
      }
    }
    if (serializedHistogram instanceof HashMap) {
      SpectatorHistogram histogram = new SpectatorHistogram();
      for (Map.Entry<?, ?> entry : ((HashMap<?, ?>) serializedHistogram).entrySet()) {
        histogram.add(entry.getKey(), (Number) entry.getValue());
      }
      return histogram;
    }
    throw new ParseException(
        null,
        "Object cannot be deserialized to a Spectator Histogram "
        + serializedHistogram.getClass()
    );
  }

  @Nullable
  static SpectatorHistogram fromByteBuffer(ByteBuffer buffer)
  {
    if (buffer == null || !buffer.hasRemaining()) {
      return null;
    }
    SpectatorHistogram histogram = new SpectatorHistogram();
    while (buffer.hasRemaining()) {
      short key = buffer.getShort();
      short idx = (short) (key & KEY_MASK);
      long val;
      if ((key & LOW_COUNT_FLAG) == LOW_COUNT_FLAG) {
        // Value/count is encoded in the top 6 bits of the short
        val = (key & VALUE_SIZE_MASK) >>> 10;
      } else {
        switch (key & VALUE_SIZE_MASK) {
          case BYTE_VALUE:
            val = buffer.get() & 0xFF;
            break;

          case SHORT_VALUE:
            val = buffer.getShort() & 0xFFFF;
            break;

          case INT_VALUE:
            val = buffer.getInt() & 0xFFFFFFFFL;
            break;

          default:
            val = buffer.getLong();
            break;
        }
      }

      histogram.add(idx, val);
    }
    if (histogram.isEmpty()) {
      return null;
    }
    return histogram;
  }

  private Short2LongOpenHashMap writableMap()
  {
    if (backingMap == null) {
      backingMap = new Short2LongOpenHashMap();
    }
    return backingMap;
  }

  private Short2LongMap readableMap()
  {
    if (isEmpty()) {
      return Short2LongMaps.EMPTY_MAP;
    }
    return backingMap;
  }

  @Nullable
  byte[] toBytes()
  {
    if (isEmpty()) {
      return null;
    }
    ByteBuffer buffer = ByteBuffer.allocate(MAX_ENTRY_BYTES * size());
    for (Short2LongMap.Entry e : Short2LongMaps.fastIterable(readableMap())) {
      short key = e.getShortKey();
      long value = e.getLongValue();
      if (value <= 0x3F) {
        // Value/count is encoded in the top 6 bits of the key bytes
        buffer.putShort((short) ((key | LOW_COUNT_FLAG) | ((int) ((value << 10) & VALUE_SIZE_MASK))));
      } else if (value <= 0xFF) {
        buffer.putShort((short) (key | BYTE_VALUE));
        buffer.put((byte) value);
      } else if (value <= 0xFFFF) {
        buffer.putShort((short) (key | SHORT_VALUE));
        buffer.putShort((short) value);
      } else if (value <= 0xFFFFFFFFL) {
        buffer.putShort((short) (key | INT_VALUE));
        buffer.putInt((int) value);
      } else {
        buffer.putShort(key);
        buffer.putLong(value);
      }
    }
    return Arrays.copyOf(buffer.array(), buffer.position());
  }

  void insert(Number num)
  {
    this.add(PercentileBuckets.indexOf(num.longValue()), 1L);
  }

  void merge(SpectatorHistogram source)
  {
    if (source == null) {
      return;
    }
    Short2LongOpenHashMap writableMap = writableMap();
    for (Short2LongMap.Entry entry : Short2LongMaps.fastIterable(source.readableMap())) {
      writableMap.addTo(entry.getShortKey(), entry.getLongValue());
      this.sumOfCounts += entry.getLongValue();
    }
  }

  // Exposed for testing
  void add(int bucket, long count)
  {
    if (bucket >= PercentileBuckets.length() || bucket < 0) {
      throw new IAE("Bucket index out of range (0, " + PercentileBuckets.length() + ")");
    }
    writableMap().addTo((short) bucket, count);
    this.sumOfCounts += count;
  }

  private void add(Object key, Number value)
  {
    if (key instanceof String) {
      this.add(Integer.parseInt((String) key), value.longValue());
      return;
    }
    if (Number.class.isAssignableFrom(key.getClass())) {
      this.add(((Number) key).intValue(), value.longValue());
      return;
    }
    throw new IAE(
        "Cannot add " + key.getClass() + "/" + value.getClass() + " to a Spectator Histogram"
    );
  }

  // Used for testing
  long get(int idx)
  {
    return readableMap().get((short) idx);
  }

  // Accessible for serialization
  void serialize(JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
  {
    JacksonUtils.writeObjectUsingSerializerProvider(jsonGenerator, serializerProvider, readableMap());
  }

  public boolean isEmpty()
  {
    return backingMap == null || backingMap.isEmpty();
  }

  public int size()
  {
    return readableMap().size();
  }

  public long getSum()
  {
    return sumOfCounts;
  }

  @Override
  public String toString()
  {
    return readableMap().toString();
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
    SpectatorHistogram that = (SpectatorHistogram) o;
    return Objects.equals(readableMap(), that.readableMap());
  }

  @Override
  public int hashCode()
  {
    return readableMap().hashCode();
  }

  @Override
  public int intValue()
  {
    return (int) getSum();
  }

  @Override
  public long longValue()
  {
    return getSum();
  }

  @Override
  public float floatValue()
  {
    return getSum();
  }

  @Override
  public double doubleValue()
  {
    return getSum();
  }

  /**
   * Compute approximate percentile for the histogram
   * @param percentile The percentile to compute
   * @return the approximate percentile
   */
  public double getPercentileValue(double percentile)
  {
    double[] pcts = new double[]{percentile};
    return getPercentileValues(pcts)[0];
  }

  /**
   * Compute approximate percentiles for the histogram
   * @param percentiles The percentiles to compute
   * @return an array of approximate percentiles in the order of those provided
   */
  public double[] getPercentileValues(double[] percentiles)
  {
    long[] counts = new long[PercentileBuckets.length()];
    for (Map.Entry<Short, Long> e : readableMap().short2LongEntrySet()) {
      counts[e.getKey()] = e.getValue();
    }
    double[] results = new double[percentiles.length];
    PercentileBuckets.percentiles(counts, percentiles, results);
    return results;
  }
}
