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

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StringSketchTest
{
  private static final int FACTOR = 2;
  private static final int NUM_STRING = StringSketch.SKETCH_K * FACTOR;
  private static final double DELTA = ItemsSketch.getNormalizedRankError(StringSketch.SKETCH_K, true) * NUM_STRING;
  private static final List<StringTuple> STRINGS = IntStream.range(0, NUM_STRING)
                                                            .mapToObj(i -> StringTuple.create(StringUtils.format("%010d", i)))
                                                            .collect(Collectors.toCollection(ArrayList::new));
  private static final StringTuple MIN_STRING = STRINGS.get(0);
  private static final StringTuple MAX_STRING = STRINGS.get(NUM_STRING - 1);

  static {
    ItemsSketch.rand.setSeed(0);  // make sketches deterministic for testing
  }

  public static class SerializationDeserializationTest
  {
    private static final ObjectMapper OBJECT_MAPPER = new JacksonModule().smileMapper(new Properties());

    @Test
    public void serializesDeserializes()
    {
      StringSketch target = new StringSketch();
      target.put(MIN_STRING);
      target.put(MAX_STRING);
      TestHelper.testSerializesDeserializes(OBJECT_MAPPER, target);
    }

    @Test
    public void abidesEqualsContract()
    {
      ItemsSketch<StringTuple> red = ItemsSketch.getInstance(StringTuple.class, StringSketch.SKETCH_K, StringSketch.STRING_TUPLE_COMPARATOR);
      red.update(StringTuple.create("a"));
      ItemsSketch<StringTuple> blue = ItemsSketch.getInstance(StringTuple.class, StringSketch.SKETCH_K, StringSketch.STRING_TUPLE_COMPARATOR);
      blue.update(StringTuple.create("b"));

      EqualsVerifier.forClass(StringSketch.class)
                    .usingGetClass()
                    .withNonnullFields("delegate")
                    .withPrefabValues(ItemsSketch.class, red, blue)
                    .verify();
    }
  }

  public static class PutTest
  {
    private StringSketch target;

    @BeforeEach
    public void setup()
    {
      target = new StringSketch();
    }

    @Test
    public void putIfNewMin()
    {
      StringTuple value = MAX_STRING;
      Assertions.assertEquals(0, getCount());

      target.putIfNewMin(value);
      Assertions.assertEquals(1, getCount());

      target.putIfNewMin(value);
      Assertions.assertEquals(1, getCount());
      Assertions.assertEquals(value, target.getDelegate().getMinItem());
      Assertions.assertEquals(value, target.getDelegate().getMaxItem());

      target.putIfNewMin(MIN_STRING);
      Assertions.assertEquals(2, getCount());
      Assertions.assertEquals(MIN_STRING, target.getDelegate().getMinItem());
      Assertions.assertEquals(MAX_STRING, target.getDelegate().getMaxItem());
    }

    @Test
    public void putIfNewMax()
    {
      StringTuple value = MIN_STRING;
      Assertions.assertEquals(0, getCount());

      target.putIfNewMax(value);
      Assertions.assertEquals(1, getCount());

      target.putIfNewMax(value);
      Assertions.assertEquals(1, getCount());
      Assertions.assertEquals(value, target.getDelegate().getMinItem());
      Assertions.assertEquals(value, target.getDelegate().getMaxItem());

      target.putIfNewMax(MAX_STRING);
      Assertions.assertEquals(2, getCount());
      Assertions.assertEquals(MIN_STRING, target.getDelegate().getMinItem());
      Assertions.assertEquals(MAX_STRING, target.getDelegate().getMaxItem());
    }

    private long getCount()
    {
      return target.getDelegate().getN();
    }
  }

  public static class PartitionTest
  {
    private static final StringSketch SKETCH;

    static {
      SKETCH = new StringSketch();
      STRINGS.forEach(SKETCH::put);
    }

    public static class TargetSizeTest
    {
      @Test
      public void requiresPositiveSize()
      {
        final IllegalArgumentException exception = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SKETCH.getEvenPartitionsByTargetSize(0)
        );
        Assertions.assertTrue(exception.getMessage().contains("targetSize must be positive but is 0"));
      }

      @Test
      public void handlesEmptySketch()
      {
        StringSketch sketch = new StringSketch();
        PartitionBoundaries partitionBoundaries = sketch.getEvenPartitionsByTargetSize(1);
        Assertions.assertEquals(0, partitionBoundaries.size());
      }

      @Test
      public void handlesSingletonSketch()
      {
        StringSketch sketch = new StringSketch();
        sketch.put(MIN_STRING);
        PartitionBoundaries partitionBoundaries = sketch.getEvenPartitionsByTargetSize(1);
        Assertions.assertEquals(2, partitionBoundaries.size());
        Assertions.assertNull(partitionBoundaries.get(0));
        Assertions.assertNull(partitionBoundaries.get(1));
      }

      @Test
      public void handlesMinimimumSize()
      {
        PartitionBoundaries partitionBoundaries = SKETCH.getEvenPartitionsByTargetSize(1);
        assertMaxNumberOfPartitions(partitionBoundaries);
      }

      @Test
      public void handlesUnevenPartitions()
      {
        List<Integer> targetSizes = Arrays.asList(127, 257, 509, 1021, 2039, 4093);
        targetSizes.forEach(TargetSizeTest::testHandlesUnevenPartitions);
      }

      private static void testHandlesUnevenPartitions(int targetSize)
      {
        PartitionBoundaries partitionBoundaries = SKETCH.getEvenPartitionsByTargetSize(targetSize);

        assertFirstAndLastPartitionsCorrect(partitionBoundaries);

        String partitionBoundariesString = PartitionTest.toString(partitionBoundaries);
        int expectedHighPartitionBoundaryCount = (int) Math.ceil((double) NUM_STRING / targetSize);
        int expectedLowPartitionBoundaryCount = expectedHighPartitionBoundaryCount - 1;
        Assertions.assertTrue(
            partitionBoundaries.size() <= expectedHighPartitionBoundaryCount + 1,
            "targetSize=" + targetSize + " " + partitionBoundariesString
        );
        Assertions.assertTrue(
            partitionBoundaries.size() >= expectedLowPartitionBoundaryCount + 1,
            "targetSize=" + targetSize + " " + partitionBoundariesString
        );

        int previous = 0;
        for (int i = 1; i < partitionBoundaries.size() - 1; i++) {
          int current = Integer.parseInt(partitionBoundaries.get(i).get(0));
          int size = current - previous;
          Assertions.assertEquals(
              targetSize,
              (double) size,
              Math.ceil(DELTA) * 2,
              getErrMsgPrefix(targetSize, i) + partitionBoundariesString
          );
          previous = current;
        }
      }

      @Test
      public void handlesSinglePartition()
      {
        PartitionBoundaries partitionBoundaries = SKETCH.getEvenPartitionsByTargetSize(NUM_STRING);
        assertSinglePartition(partitionBoundaries);
      }

      @Test
      public void handlesOversizedPartition()
      {
        PartitionBoundaries partitionBoundaries = SKETCH.getEvenPartitionsByTargetSize(Integer.MAX_VALUE);
        assertSinglePartition(partitionBoundaries);
      }
    }

    public static class MaxSizeTest
    {
      @Test
      public void requiresPositiveSize()
      {
        final IllegalArgumentException exception = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SKETCH.getEvenPartitionsByMaxSize(0)
        );
        Assertions.assertTrue(exception.getMessage().contains("maxSize must be positive but is 0"));
      }

      @Test
      public void handlesEmptySketch()
      {
        StringSketch sketch = new StringSketch();
        PartitionBoundaries partitionBoundaries = sketch.getEvenPartitionsByMaxSize(1);
        Assertions.assertEquals(0, partitionBoundaries.size());
      }

      @Test
      public void handlesSingletonSketch()
      {
        StringSketch sketch = new StringSketch();
        sketch.put(MIN_STRING);
        PartitionBoundaries partitionBoundaries = sketch.getEvenPartitionsByMaxSize(1);
        Assertions.assertEquals(2, partitionBoundaries.size());
        Assertions.assertNull(partitionBoundaries.get(0));
        Assertions.assertNull(partitionBoundaries.get(1));
      }

      @Test
      public void handlesMinimimumSize()
      {
        PartitionBoundaries partitionBoundaries = SKETCH.getEvenPartitionsByMaxSize(1);
        assertMaxNumberOfPartitions(partitionBoundaries);
      }

      @Test
      public void handlesUnevenPartitions()
      {
        List<Integer> maxSizes = Arrays.asList(509, 1021, 2039, 4093);
        maxSizes.forEach(MaxSizeTest::testHandlesUnevenPartitions);
      }

      private static void testHandlesUnevenPartitions(int maxSize)
      {
        PartitionBoundaries partitionBoundaries = SKETCH.getEvenPartitionsByMaxSize(maxSize);

        assertFirstAndLastPartitionsCorrect(partitionBoundaries);

        String partitionBoundariesString = PartitionTest.toString(partitionBoundaries);
        long expectedPartitionCount = (long) Math.ceil((double) NUM_STRING / maxSize);
        Assertions.assertEquals(
            expectedPartitionCount + 1,
            partitionBoundaries.size(),
            "maxSize=" + maxSize + " " + partitionBoundariesString
        );

        double minSize = (double) NUM_STRING / expectedPartitionCount - DELTA;

        int previous = 0;
        for (int i = 1; i < partitionBoundaries.size() - 1; i++) {
          int current = Integer.parseInt(partitionBoundaries.get(i).get(0));
          int size = current - previous;
          Assertions.assertTrue(
              size <= maxSize,
              getErrMsgPrefix(maxSize, i) + partitionBoundariesString
          );
          Assertions.assertTrue(
              size >= minSize,
              getErrMsgPrefix(maxSize, i) + partitionBoundariesString
          );
          previous = current;
        }
      }

      @Test
      public void handlesSinglePartition()
      {
        PartitionBoundaries partitionBoundaries = SKETCH.getEvenPartitionsByMaxSize(
            (int) Math.ceil(NUM_STRING + DELTA)
        );
        assertSinglePartition(partitionBoundaries);
      }

      @Test
      public void handlesOversizedPartition()
      {
        PartitionBoundaries partitionBoundaries = SKETCH.getEvenPartitionsByMaxSize(Integer.MAX_VALUE);
        assertSinglePartition(partitionBoundaries);
      }
    }

    private static void assertMaxNumberOfPartitions(PartitionBoundaries partitionBoundaries)
    {
      String partitionBoundariesString = toString(partitionBoundaries);

      Assertions.assertEquals(
          StringSketch.SKETCH_K + 1,
          partitionBoundaries.size(),
          partitionBoundariesString
      );
      assertFirstAndLastPartitionsCorrect(partitionBoundaries);

      int previous = 0;
      for (int i = 1; i < partitionBoundaries.size() - 1; i++) {
        int current = Integer.parseInt(partitionBoundaries.get(i).get(0));
        Assertions.assertEquals(
            1,
            current - previous,
            FACTOR,
            getErrMsgPrefix(1, i) + partitionBoundariesString
        );
        previous = current;
      }
    }

    private static void assertSinglePartition(PartitionBoundaries partitionBoundaries)
    {
      Assertions.assertEquals(2, partitionBoundaries.size());
      assertFirstAndLastPartitionsCorrect(partitionBoundaries);
    }

    private static void assertFirstAndLastPartitionsCorrect(PartitionBoundaries partitionBoundaries)
    {
      Assertions.assertNull(partitionBoundaries.get(0));
      Assertions.assertNull(partitionBoundaries.get(partitionBoundaries.size() - 1));
    }

    private static String getErrMsgPrefix(int size, int i)
    {
      return "size=" + size + " i=" + i + " of ";
    }

    private static String toString(PartitionBoundaries partitionBoundaries)
    {
      String prefix = "partitionBoundaries[" + partitionBoundaries.size() + "]=";
      StringJoiner sj = new StringJoiner(" ", prefix, "]");
      for (int i = 0; i < partitionBoundaries.size(); i++) {
        sj.add("[" + i + "]=" + partitionBoundaries.get(i));
      }
      return sj.toString();
    }
  }
}
