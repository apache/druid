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
import com.google.common.collect.Lists;
import io.druid.jackson.DefaultObjectMapper;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

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
  public void test()
  {
    Random r = new Random();
    List<Float> inputs = Lists.newArrayListWithExpectedSize(4096);
    while (inputs.size() < 4096) {
      float v = r.nextFloat() * 100 - 30;
      do {
        inputs.add(v);
      } while (r.nextFloat() > 0.8f);
    }
    Collections.shuffle(inputs);

    List<Integer> lengths1 = Lists.newArrayList();
    List<Float> quantiles1 = Lists.newArrayList();
    for (int size : new int[]{1, 8, 64, 512, 4096}) {
      for (int z : new int[]{16, 64, 256, 1024}) {
        ApproximateHistogramHolder h = new ApproximateHistogram(z);
        for (int j = 0; j < size; j++) {
          h.offer(inputs.get(j));
        }
        lengths1.add(h.toBytes().length);
        for (float f : new float[]{0.25f, 0.5f, 0.75f}) {
          quantiles1.add(h.getQuantiles(new float[]{f})[0]);
        }
      }
    }

    List<Integer> lengths2 = Lists.newArrayList();
    List<Float> quantiles2 = Lists.newArrayList();
    for (int size : new int[]{1, 8, 64, 512, 4096}) {
      for (int z : new int[]{16, 64, 256, 1024}) {
        ApproximateHistogramHolder h = new ApproximateCompactHistogram(z);
        for (int j = 0; j < size; j++) {
          h.offer(inputs.get(j));
        }
        lengths2.add(h.toBytes().length);

        h = h.fromBytes(h.toBytes());
        for (float f : new float[]{0.25f, 0.5f, 0.75f}) {
          quantiles2.add(h.getQuantiles(new float[]{f})[0]);
        }
      }
    }
    Assert.assertEquals(quantiles1, quantiles2);

    for (int i = 0; i < lengths1.size(); i++) {
      int s1 = lengths1.get(i);
      int s2 = lengths2.get(i);
      System.out.println(s1 + " --> " + s2 + " :: " + ((float)(s2 - s1)/ (float)s1 * 100) + "%");
    }
  }
}
