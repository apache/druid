/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class HistogramTest
{
  @Test
  public void testOffer() {
    final float[] values = {0.55f, 0.27f, -0.3f, -.1f, -0.8f, -.7f, -.5f, 0.25f, 0.1f, 2f, -3f};
    final float[] breaks = {-1f, -0.5f, 0.0f, 0.5f, 1f};

    Histogram hExpected = new Histogram(breaks, new long[]{1,3,2,3,1,1}, -3f, 2f);

    Histogram h = new Histogram(breaks);
    for(float v : values) h.offer(v);

    Assert.assertEquals("histogram matches expected histogram", hExpected, h);
  }

  @Test
  public void testToFromBytes() {
    float[] breaks = {-1f, -0.5f, 0.0f, 0.5f, 1f};
    long [] bins   = { 23, 123, 4, 56, 7, 493210};
    Histogram h = new Histogram(breaks, bins, -1f, 1f);

    Assert.assertEquals(Histogram.fromBytes(h.toBytes()), h);
  }

  @Test
  public void testAsVisual() throws Exception {
    float[] breaks = {-1f, -0.5f, 0.0f, 0.5f, 1f};
    long [] bins   = { 23, 123, 4, 56, 7, 493210};
    Histogram h = new Histogram(breaks, bins, -1f, 1f);

    Double[] visualBreaks = {-1.0, -0.5, 0.0, 0.5, 1.0};
    Double[] visualCounts = { 123., 4., 56., 7. };

    ObjectMapper objectMapper = new DefaultObjectMapper();
    String json = objectMapper.writeValueAsString(h.asVisual());

    Map<String,Object> expectedObj = Maps.newLinkedHashMap();
    expectedObj.put("breaks", Arrays.asList(visualBreaks));
    expectedObj.put("counts", Arrays.asList(visualCounts));
    expectedObj.put("quantiles", Arrays.asList(new Double[]{-1.0, 1.0}));

    Map<String,Object> obj = (Map<String, Object>)objectMapper.readValue(json, Object.class);
    Assert.assertEquals(expectedObj, obj);
  }
}
