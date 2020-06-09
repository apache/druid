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

package org.apache.druid.indexing.common.task.batch.parallel.distribution;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.druid.timeline.partition.PartitionBoundaries;

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

/**
 * Counts approximate frequencies of strings.
 */
@JsonSerialize(using = StringSketch.Jackson.Serializer.class)
@JsonDeserialize(using = StringSketch.Jackson.Deserializer.class)
public class StringSketch implements StringDistribution
{
  static final String NAME = "sketch";
  static final int SKETCH_K = 1 << 12;  // smallest value with normalized rank error < 0.1%; retain up to ~86k elements
  static final Comparator<String> SKETCH_COMPARATOR = Comparator.naturalOrder();
  private static final ArrayOfStringsSerDe ARRAY_OF_STRINGS_SERDE = new ArrayOfStringsSerDe();

  private final ItemsSketch<String> delegate;

  public StringSketch()
  {
    this(ItemsSketch.getInstance(SKETCH_K, SKETCH_COMPARATOR));
  }

  StringSketch(ItemsSketch<String> sketch)
  {
    this.delegate = sketch;
  }

  @Override
  public void put(String string)
  {
    delegate.update(string);
  }

  @Override
  public void putIfNewMin(String string)
  {
    String min = delegate.getMinValue();
    if (min == null || string.compareTo(min) < 0) {
      delegate.update(string);
    }
  }

  @Override
  public void putIfNewMax(String string)
  {
    String max = delegate.getMaxValue();
    if (max == null || string.compareTo(max) > 0) {
      delegate.update(string);
    }
  }

  @Override
  public PartitionBoundaries getEvenPartitionsByMaxSize(int maxSize)
  {
    Preconditions.checkArgument(maxSize > 0, "maxSize must be positive but is %s", maxSize);
    long n = delegate.getN();
    double delta = delegate.getNormalizedRankError(true) * n;  // account for approx distribution
    int targetSize = Math.max(1, (int) Math.floor(maxSize - delta));  // floor() to increase chance below max size
    int evenPartitionCount = (int) Math.ceil((double) n / targetSize);  // ceil() to increase chance below max size
    return getEvenPartitionsByCount(Math.max(1, evenPartitionCount));
  }

  @Override
  public PartitionBoundaries getEvenPartitionsByTargetSize(int targetSize)
  {
    Preconditions.checkArgument(targetSize > 0, "targetSize must be positive but is %s", targetSize);
    long n = delegate.getN();
    int evenPartitionCount = Math.max(1, (int) Math.round((double) n / targetSize));
    return getEvenPartitionsByCount(evenPartitionCount);
  }

  @VisibleForTesting
  public String getMin()
  {
    return delegate.getMinValue();
  }

  @VisibleForTesting
  public String getMax()
  {
    return delegate.getMaxValue();
  }

  private PartitionBoundaries getEvenPartitionsByCount(int evenPartitionCount)
  {
    Preconditions.checkArgument(
        evenPartitionCount > 0,
        "evenPartitionCount must be positive but is %s",
        evenPartitionCount
    );
    String[] partitions = delegate.getQuantiles(evenPartitionCount + 1); // add 1 since this returns endpoints
    return new PartitionBoundaries((partitions == null) ? new String[0] : partitions);
  }

  @Override
  public String toString()
  {
    return "StringSketch{" +
           "delegate=" + delegate +
           '}';
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
    StringSketch that = (StringSketch) o;

    // ParallelIndexPhaseRunner.collectReport() uses equals() to check subtasks send identical reports if they retry.
    // However, ItemsSketch does not override equals(): https://github.com/apache/incubator-datasketches-java/issues/140
    //
    // Since ItemsSketch has built-in non-determinism, only rely on ItemsSketch properties that are deterministic. This
    // check is best-effort as it is possible for it to return true for sketches that contain different values.
    return delegate.getK() == that.delegate.getK() &&
           delegate.getN() == that.delegate.getN() &&
           Objects.equals(delegate.getMaxValue(), that.delegate.getMaxValue()) &&
           Objects.equals(delegate.getMinValue(), that.delegate.getMinValue());
  }

  @Override
  public int hashCode()
  {
    // See comment in equals() regarding ItemsSketch.
    return Objects.hash(
        delegate.getK(),
        delegate.getN(),
        delegate.getMaxValue(),
        delegate.getMinValue()
    );
  }

  ItemsSketch<String> getDelegate()
  {
    return delegate;
  }

  private byte[] toByteArray()
  {
    return delegate.toByteArray(ARRAY_OF_STRINGS_SERDE);
  }

  static class Jackson
  {
    private static final String FIELD_SKETCH = "sketch";

    static class Serializer extends StdSerializer<StringSketch>
    {
      Serializer()
      {
        super(StringSketch.class);
      }

      @Override
      public void serialize(
          StringSketch stringSketch,
          JsonGenerator jsonGenerator,
          SerializerProvider serializerProvider
      ) throws IOException
      {
        jsonGenerator.writeBinaryField(FIELD_SKETCH, stringSketch.toByteArray());
      }

      @Override
      public void serializeWithType(
          StringSketch value,
          JsonGenerator gen,
          SerializerProvider serializers,
          TypeSerializer typeSer
      ) throws IOException
      {
        typeSer.writeTypePrefixForObject(value, gen);
        serialize(value, gen, serializers);
        typeSer.writeTypeSuffixForObject(value, gen);
      }
    }

    static class Deserializer extends StdDeserializer<StringSketch>
    {
      Deserializer()
      {
        super(StringSketch.class);
      }

      @Override
      public StringSketch deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
          throws IOException
      {
        JsonNode jsonNode = jsonParser.getCodec().readTree(jsonParser);
        byte[] sketchBytes = jsonNode.get(FIELD_SKETCH).binaryValue();
        ItemsSketch<String> sketch = ItemsSketch.getInstance(
            Memory.wrap(sketchBytes),
            SKETCH_COMPARATOR,
            ARRAY_OF_STRINGS_SERDE
        );
        return new StringSketch(sketch);
      }
    }
  }
}
