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

package io.druid.query.aggregation.histogram;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class QuantilesTest
{
  @Test
  public void testSerialization() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    float[] probabilities = new float[]{0.25f, 0.5f, 0.75f};
    float[] quantiles = new float[]{0.25f, 0.5f, 0.75f};
    float min = 0f;
    float max = 4f;

    String theString = mapper.writeValueAsString(
        new Quantiles(probabilities, quantiles, min, max)
    );

    Object theObject = mapper.readValue(theString, Object.class);
    Assert.assertThat(theObject, CoreMatchers.instanceOf(LinkedHashMap.class));

    LinkedHashMap theMap = (LinkedHashMap) theObject;

    ArrayList theProbabilities = (ArrayList<Float>) theMap.get("probabilities");

    Assert.assertEquals(probabilities.length, theProbabilities.size());
    for (int i = 0; i < theProbabilities.size(); ++i) {
      Assert.assertEquals(probabilities[i], ((Number) theProbabilities.get(i)).floatValue(), 0.0001f);
    }

    ArrayList theQuantiles = (ArrayList<Float>) theMap.get("quantiles");

    Assert.assertEquals(quantiles.length, theQuantiles.size());
    for (int i = 0; i < theQuantiles.size(); ++i) {
      Assert.assertEquals(quantiles[i], ((Number) theQuantiles.get(i)).floatValue(), 0.0001f);
    }

    Assert.assertEquals(
        "serialized min. matches expected min.",
        min,
        ((Number) theMap.get("min")).floatValue(),
        0.0001f
    );
    Assert.assertEquals(
        "serialized max. matches expected max.",
        max,
        ((Number) theMap.get("max")).floatValue(),
        0.0001f
    );
  }

  @Test
  public void testMedianDiffQuantile1() throws Exception
  {
    ApproximateHistogram h = new ApproximateHistogram();
    h.offer(100);
    h.offer(200);
    h.offer(200);
    // 100 <200> 200
    Assert.assertEquals(200.0f, h.getMedian(), 0.0000001f);
    Assert.assertEquals(141.42136f, h.getQuantile(0.5f), 0.0000001f);
    Assert.assertNotEquals(h.getQuantile(0.5f), h.getMedian(), 0.00001f);

    h.offer(100);
    // 100 100 <> 200 200
    Assert.assertEquals(150.0f, h.getMedian(), 0.0000001f);
    Assert.assertEquals(100.0f, h.getQuantile(0.5f), 0.0000001f);
    Assert.assertNotEquals(h.getQuantile(0.5f), h.getMedian(), 0.00001f);

    h.offer(300);
    h.offer(200);
    // 100 100 200 <> 200 200 300
    Assert.assertEquals(200.0f, h.getMedian(), 0.0000001f);
    Assert.assertEquals(144.94897f, h.getQuantile(0.5f), 0.0000001f);
    Assert.assertNotEquals(h.getQuantile(0.5f), h.getMedian(), 0.00001f);

    h.offer(400);
    h.offer(500);
    // 100 100 200 200 <> 200 300 400 500
    Assert.assertEquals(200.0f, h.getMedian(), 0.0000001f);
    Assert.assertEquals(182.84271f, h.getQuantile(0.5f), 0.0000001f);
    Assert.assertNotEquals(h.getQuantile(0.5f), h.getMedian(), 0.00001f);

    h.offer(400);
    h.offer(500);
    // 100 100 200 200 200 <> 300 400 400 500 500
    Assert.assertEquals(250.0f, h.getMedian(), 0.0000001f);
    Assert.assertEquals(200.0f, h.getQuantile(0.5f), 0.0000001f);
    Assert.assertNotEquals(h.getQuantile(0.5f), h.getMedian(), 0.00001f);

    h.offer(350);
    // 100 100 200 200 200 <300> 350 400 400 500 500
    Assert.assertEquals(300.0f, h.getMedian(), 0.0000001f);
    Assert.assertEquals(217.71243f, h.getQuantile(0.5f), 0.0000001f);
    Assert.assertNotEquals(h.getQuantile(0.5f), h.getMedian(), 0.00001f);
  }

  @Test
  public void testMedianDiffQuantile2() throws Exception
  {
    // with approx positions
    ApproximateHistogram h = new ApproximateHistogram(3);
    h.offer(100);
    h.offer(200);
    h.offer(200);
    h.offer(300);
    h.offer(300);
    h.offer(110);
    // 100* 110* 200 <> 200 300 300 : positions=[105.0, 200.0, 300.0], bins=[*2, 2, 2]
    Assert.assertEquals(200.0f, h.getMedian(), 0.00001f);
    Assert.assertEquals(152.5f, h.getQuantile(0.5f), 0.00001f);
    Assert.assertNotEquals(h.getQuantile(0.5f), h.getMedian(), 0.00001f);

    h.offer(120);
    // 100* 110* 120* <200> 200 300 300 : positions=[110.0, 200.0, 300.0], bins=[*3, 2, 2]
    Assert.assertEquals(200.0f, h.getMedian(), 0.00001f);
    Assert.assertEquals(125.44156f, h.getQuantile(0.5f), 0.00001f);
    Assert.assertNotEquals(h.getQuantile(0.5f), h.getMedian(), 0.00001f);

    h.offer(210);
    // 100* 110* 120* 200* <> 200* 210* 300 300 : positions=[110.0, 203.33333, 300.0], bins=[*3, *3, 2]
    // fallback to quantile(0.5)
    Assert.assertEquals(141.11111, h.getQuantile(0.5f), 0.00001f);
    Assert.assertEquals(h.getQuantile(0.5f), h.getMedian(), 0.00001f);
  }
}
