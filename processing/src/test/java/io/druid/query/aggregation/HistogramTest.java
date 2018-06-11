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
import com.google.common.collect.Maps;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class HistogramTest
{
  @Test
  public void testOffer()
  {
    final float[] values = {0.55f, 0.27f, -0.3f, -.1f, -0.8f, -.7f, -.5f, 0.25f, 0.1f, 2f, -3f};
    final float[] breaks = {-1f, -0.5f, 0.0f, 0.5f, 1f};

    Histogram hExpected = new Histogram(breaks, new long[]{1, 3, 2, 3, 1, 1}, -3f, 2f);

    Histogram h = new Histogram(breaks);
    for (float v : values) {
      h.offer(v);
    }

    Assert.assertEquals("histogram matches expected histogram", hExpected, h);
  }

  /**
   * This test differs from {@link #testOffer()} only in that it offers only negative values into Histogram. It's to
   * expose the issue of using Float's MIN_VALUE that is actually positive as initial value for {@link Histogram#max}.
   */
  @Test
  public void testOfferOnlyNegative()
  {
    final float[] values = {-0.3f, -.1f, -0.8f, -.7f, -.5f, -3f};
    final float[] breaks = {-1f, -0.5f, 0.0f, 0.5f, 1f};

    Histogram hExpected = new Histogram(breaks, new long[]{1, 3, 2, 0, 0, 0}, -3f, -0.1f);

    Histogram h = new Histogram(breaks);
    for (float v : values) {
      h.offer(v);
    }

    Assert.assertEquals("histogram matches expected histogram", hExpected, h);
  }

  @Test
  public void testToFromBytes()
  {
    float[] breaks = {-1f, -0.5f, 0.0f, 0.5f, 1f};
    long[] bins = {23, 123, 4, 56, 7, 493210};
    Histogram h = new Histogram(breaks, bins, -1f, 1f);

    Assert.assertEquals(Histogram.fromBytes(h.toBytes()), h);
  }

  @Test
  public void testAsVisual() throws Exception
  {
    float[] breaks = {-1f, -0.5f, 0.0f, 0.5f, 1f};
    long[] bins = {23, 123, 4, 56, 7, 493210};
    Histogram h = new Histogram(breaks, bins, -1f, 1f);

    Double[] visualBreaks = {-1.0, -0.5, 0.0, 0.5, 1.0};
    Double[] visualCounts = {123., 4., 56., 7.};

    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    String json = objectMapper.writeValueAsString(h.asVisual());

    Map<String, Object> expectedObj = Maps.newLinkedHashMap();
    expectedObj.put("breaks", Arrays.asList(visualBreaks));
    expectedObj.put("counts", Arrays.asList(visualCounts));
    expectedObj.put("quantiles", Arrays.asList(new Double[]{-1.0, 1.0}));

    Map<String, Object> obj = (Map<String, Object>) objectMapper.readValue(json, Object.class);
    Assert.assertEquals(expectedObj, obj);
  }
}
