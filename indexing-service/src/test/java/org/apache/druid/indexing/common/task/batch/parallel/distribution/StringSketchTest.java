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
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.hamcrest.Matchers;
import org.hamcrest.number.IsCloseTo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(Enclosed.class)
public class StringSketchTest
{
  private static final int FACTOR = 2;
  private static final int NUM_STRING = StringSketch.SKETCH_K * FACTOR;
  private static final double DELTA = ItemsSketch.getNormalizedRankError(StringSketch.SKETCH_K, true) * NUM_STRING;
  private static final List<String> STRINGS = IntStream.range(0, NUM_STRING)
                                                       .mapToObj(i -> StringUtils.format("%010d", i))
                                                       .collect(Collectors.toCollection(ArrayList::new));
  private static final String MIN_STRING = STRINGS.get(0);
  private static final String MAX_STRING = STRINGS.get(NUM_STRING - 1);

  static {
    ItemsSketch.rand.setSeed(0);  // make sketches deterministic for testing
  }

  public static class SerializationDeserializationTest
  {
    private static final ObjectMapper OBJECT_MAPPER = new JacksonModule().smileMapper();

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
      EqualsVerifier.forClass(StringSketch.class)
                    .usingGetClass()
                    .withNonnullFields("delegate")
                    .verify();
    }
  }

  public static class PutTest
  {
    private StringSketch target;

    @Before
    public void setup()
    {
      target = new StringSketch();
    }

    @Test
    public void putIfNewMin()
    {
      String value = MAX_STRING;
      Assert.assertEquals(0, getCount());

      target.putIfNewMin(value);
      Assert.assertEquals(1, getCount());

      target.putIfNewMin(value);
      Assert.assertEquals(1, getCount());
      Assert.assertEquals(value, target.getDelegate().getMinValue());
      Assert.assertEquals(value, target.getDelegate().getMaxValue());

      target.putIfNewMin(MIN_STRING);
      Assert.assertEquals(2, getCount());
      Assert.assertEquals(MIN_STRING, target.getDelegate().getMinValue());
      Assert.assertEquals(MAX_STRING, target.getDelegate().getMaxValue());
    }

    @Test
    public void putIfNewMax()
    {
      String value = MIN_STRING;
      Assert.assertEquals(0, getCount());

      target.putIfNewMax(value);
      Assert.assertEquals(1, getCount());

      target.putIfNewMax(value);
      Assert.assertEquals(1, getCount());
      Assert.assertEquals(value, target.getDelegate().getMinValue());
      Assert.assertEquals(value, target.getDelegate().getMaxValue());

      target.putIfNewMax(MAX_STRING);
      Assert.assertEquals(2, getCount());
      Assert.assertEquals(MIN_STRING, target.getDelegate().getMinValue());
      Assert.assertEquals(MAX_STRING, target.getDelegate().getMaxValue());
    }

    private long getCount()
    {
      return target.getDelegate().getN();
    }
  }

  @RunWith(Enclosed.class)
  public static class PartitionTest
  {
    private static final StringSketch SKETCH;

    static {
      SKETCH = new StringSketch();
      STRINGS.forEach(SKETCH::put);
    }

    public static class TargetSizeTest
    {
      @Rule
      public ExpectedException exception = ExpectedException.none();

      @Test
      public void requiresPositiveSize()
      {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("targetSize must be positive but is 0");

        SKETCH.getEvenPartitionsByTargetSize(0);
      }

      @Test
      public void handlesEmptySketch()
      {
        StringSketch sketch = new StringSketch();
        PartitionBoundaries partitionBoundaries = sketch.getEvenPartitionsByTargetSize(1);
        Assert.assertEquals(0, partitionBoundaries.size());
      }

      @Test
      public void handlesSingletonSketch()
      {
        StringSketch sketch = new StringSketch();
        sketch.put(MIN_STRING);
        PartitionBoundaries partitionBoundaries = sketch.getEvenPartitionsByTargetSize(1);
        Assert.assertEquals(2, partitionBoundaries.size());
        Assert.assertNull(partitionBoundaries.get(0));
        Assert.assertNull(partitionBoundaries.get(1));
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
        Assert.assertThat(
            "targetSize=" + targetSize + " " + partitionBoundariesString,
            partitionBoundaries.size(),
            Matchers.lessThanOrEqualTo(expectedHighPartitionBoundaryCount + 1)
        );
        Assert.assertThat(
            "targetSize=" + targetSize + " " + partitionBoundariesString,
            partitionBoundaries.size(),
            Matchers.greaterThanOrEqualTo(expectedLowPartitionBoundaryCount + 1)
        );

        int previous = 0;
        for (int i = 1; i < partitionBoundaries.size() - 1; i++) {
          int current = Integer.parseInt(partitionBoundaries.get(i));
          int size = current - previous;
          Assert.assertThat(
              getErrMsgPrefix(targetSize, i) + partitionBoundariesString,
              (double) size,
              IsCloseTo.closeTo(targetSize, Math.ceil(DELTA) * 2)
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
      @Rule
      public ExpectedException exception = ExpectedException.none();

      @Test
      public void requiresPositiveSize()
      {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("maxSize must be positive but is 0");

        SKETCH.getEvenPartitionsByMaxSize(0);
      }

      @Test
      public void handlesEmptySketch()
      {
        StringSketch sketch = new StringSketch();
        PartitionBoundaries partitionBoundaries = sketch.getEvenPartitionsByMaxSize(1);
        Assert.assertEquals(0, partitionBoundaries.size());
      }

      @Test
      public void handlesSingletonSketch()
      {
        StringSketch sketch = new StringSketch();
        sketch.put(MIN_STRING);
        PartitionBoundaries partitionBoundaries = sketch.getEvenPartitionsByMaxSize(1);
        Assert.assertEquals(2, partitionBoundaries.size());
        Assert.assertNull(partitionBoundaries.get(0));
        Assert.assertNull(partitionBoundaries.get(1));
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
        Assert.assertEquals(
            "maxSize=" + maxSize + " " + partitionBoundariesString,
            expectedPartitionCount + 1,
            partitionBoundaries.size()
        );

        double minSize = (double) NUM_STRING / expectedPartitionCount - DELTA;

        int previous = 0;
        for (int i = 1; i < partitionBoundaries.size() - 1; i++) {
          int current = Integer.parseInt(partitionBoundaries.get(i));
          int size = current - previous;
          Assert.assertThat(
              getErrMsgPrefix(maxSize, i) + partitionBoundariesString,
              size,
              Matchers.lessThanOrEqualTo(maxSize)
          );
          Assert.assertThat(
              getErrMsgPrefix(maxSize, i) + partitionBoundariesString,
              (double) size,
              Matchers.greaterThanOrEqualTo(minSize)
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

      Assert.assertEquals(partitionBoundariesString, StringSketch.SKETCH_K + 1, partitionBoundaries.size());
      assertFirstAndLastPartitionsCorrect(partitionBoundaries);

      int previous = 0;
      for (int i = 1; i < partitionBoundaries.size() - 1; i++) {
        int current = Integer.parseInt(partitionBoundaries.get(i));
        Assert.assertEquals(
            getErrMsgPrefix(1, i) + partitionBoundariesString,
            1,
            current - previous,
            FACTOR
        );
        previous = current;
      }
    }

    private static void assertSinglePartition(PartitionBoundaries partitionBoundaries)
    {
      Assert.assertEquals(2, partitionBoundaries.size());
      assertFirstAndLastPartitionsCorrect(partitionBoundaries);
    }

    private static void assertFirstAndLastPartitionsCorrect(PartitionBoundaries partitionBoundaries)
    {
      Assert.assertNull(partitionBoundaries.get(0));
      Assert.assertNull(partitionBoundaries.get(partitionBoundaries.size() - 1));
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
