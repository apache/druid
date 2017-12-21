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

package io.druid.query.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Floats;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class HistogramAggregatorTest
{
  private void aggregate(TestFloatColumnSelector selector, Aggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    String json0 = "{\"type\": \"histogram\", \"name\": \"billy\", \"fieldName\": \"nilly\"}";
    HistogramAggregatorFactory agg0 = objectMapper.readValue(json0, HistogramAggregatorFactory.class);
    Assert.assertEquals(ImmutableList.of(), agg0.getBreaks());

    String aggSpecJson = "{\"type\": \"histogram\", \"name\": \"billy\", \"fieldName\": \"nilly\", \"breaks\": [ -1, 2, 3.0 ]}";
    HistogramAggregatorFactory agg = objectMapper.readValue(aggSpecJson, HistogramAggregatorFactory.class);

    Assert.assertEquals(new HistogramAggregatorFactory("billy", "nilly", Arrays.asList(-1f, 2f, 3.0f)), agg);
    Assert.assertEquals(agg, objectMapper.readValue(objectMapper.writeValueAsBytes(agg), HistogramAggregatorFactory.class));
  }

  @Test
  public void testAggregate() throws Exception
  {
    final float[] values = {0.55f, 0.27f, -0.3f, -.1f, -0.8f, -.7f, -.5f, 0.25f, 0.1f, 2f, -3f};
    final float[] breaks = {-1f, -0.5f, 0.0f, 0.5f, 1f};

    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    HistogramAggregator agg = new HistogramAggregator(selector, breaks);

    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 0, 0}, ((Histogram) agg.get()).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 0, 0}, ((Histogram) agg.get()).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 0, 0}, ((Histogram) agg.get()).bins);
    aggregate(selector, agg);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 1, 0}, ((Histogram) agg.get()).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 1, 0}, ((Histogram) agg.get()).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 1, 0}, ((Histogram) agg.get()).bins);
    aggregate(selector, agg);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 1, 1, 0}, ((Histogram) agg.get()).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 1, 1, 0}, ((Histogram) agg.get()).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 1, 1, 0}, ((Histogram) agg.get()).bins);
    aggregate(selector, agg);
    Assert.assertArrayEquals(new long[]{0, 0, 1, 1, 1, 0}, ((Histogram) agg.get()).bins);
    aggregate(selector, agg);
    Assert.assertArrayEquals(new long[]{0, 0, 2, 1, 1, 0}, ((Histogram) agg.get()).bins);
    aggregate(selector, agg);
    Assert.assertArrayEquals(new long[]{0, 1, 2, 1, 1, 0}, ((Histogram) agg.get()).bins);
    aggregate(selector, agg);
    Assert.assertArrayEquals(new long[]{0, 2, 2, 1, 1, 0}, ((Histogram) agg.get()).bins);
    aggregate(selector, agg);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 1, 1, 0}, ((Histogram) agg.get()).bins);
    aggregate(selector, agg);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 2, 1, 0}, ((Histogram) agg.get()).bins);
    aggregate(selector, agg);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 3, 1, 0}, ((Histogram) agg.get()).bins);
    aggregate(selector, agg);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 3, 1, 1}, ((Histogram) agg.get()).bins);
    aggregate(selector, agg);
    Assert.assertArrayEquals(new long[]{1, 3, 2, 3, 1, 1}, ((Histogram) agg.get()).bins);
  }

  private void aggregateBuffer(TestFloatColumnSelector selector, BufferAggregator agg, ByteBuffer buf, int position)
  {
    agg.aggregate(buf, position);
    selector.increment();
  }

  @Test
  public void testBufferAggregate() throws Exception
  {
    final float[] values = {0.55f, 0.27f, -0.3f, -.1f, -0.8f, -.7f, -.5f, 0.25f, 0.1f, 2f, -3f};
    final float[] breaks = {-1f, -0.5f, 0.0f, 0.5f, 1f};

    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    HistogramAggregatorFactory factory = new HistogramAggregatorFactory(
        "billy",
        "billy",
        Floats.asList(breaks)
    );
    HistogramBufferAggregator agg = new HistogramBufferAggregator(selector, breaks);

    ByteBuffer buf = ByteBuffer.allocateDirect(factory.getMaxIntermediateSize());
    int position = 0;

    agg.init(buf, position);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 0, 0}, ((Histogram) agg.get(buf, position)).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 0, 0}, ((Histogram) agg.get(buf, position)).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 0, 0}, ((Histogram) agg.get(buf, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 1, 0}, ((Histogram) agg.get(buf, position)).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 1, 0}, ((Histogram) agg.get(buf, position)).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 1, 0}, ((Histogram) agg.get(buf, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 1, 1, 0}, ((Histogram) agg.get(buf, position)).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 1, 1, 0}, ((Histogram) agg.get(buf, position)).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 1, 1, 0}, ((Histogram) agg.get(buf, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 0, 1, 1, 1, 0}, ((Histogram) agg.get(buf, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 0, 2, 1, 1, 0}, ((Histogram) agg.get(buf, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 1, 2, 1, 1, 0}, ((Histogram) agg.get(buf, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 2, 2, 1, 1, 0}, ((Histogram) agg.get(buf, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 1, 1, 0}, ((Histogram) agg.get(buf, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 2, 1, 0}, ((Histogram) agg.get(buf, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 3, 1, 0}, ((Histogram) agg.get(buf, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 3, 1, 1}, ((Histogram) agg.get(buf, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{1, 3, 2, 3, 1, 1}, ((Histogram) agg.get(buf, position)).bins);
  }
}
