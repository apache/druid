package com.metamx.druid.histogram;

import com.google.common.collect.Maps;
import com.metamx.druid.aggregation.Histogram;
import com.metamx.druid.jackson.DefaultObjectMapper;
import org.codehaus.jackson.map.ObjectMapper;
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
    expectedObj.put("min", -1.0);
    expectedObj.put("max", 1.0);

    Map<String,Object> obj = (Map<String, Object>)objectMapper.readValue(json, Object.class);
    Assert.assertEquals(expectedObj, obj);
  }
}
