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

package org.apache.druid.collections.spatial.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class RectangularBoundTest
{
  @Test
  public void testCacheKey()
  {
    Assert.assertArrayEquals(
        new RectangularBound(new float[]{1F, 1F}, new float[]{2F, 2F}, 1).getCacheKey(),
        new RectangularBound(new float[]{1F, 1F}, new float[]{2F, 2F}, 1).getCacheKey()
    );
    Assert.assertFalse(Arrays.equals(
        new RectangularBound(new float[]{1F, 1F}, new float[]{2F, 2F}, 1).getCacheKey(),
        new RectangularBound(new float[]{1F, 1F}, new float[]{2F, 3F}, 1).getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        new RectangularBound(new float[]{1F, 1F}, new float[]{2F, 2F}, 1).getCacheKey(),
        new RectangularBound(new float[]{1F, 0F}, new float[]{2F, 2F}, 1).getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        new RectangularBound(new float[]{1F, 1F}, new float[]{2F, 2F}, 1).getCacheKey(),
        new RectangularBound(new float[]{1F, 1F}, new float[]{2F, 2F}, 2).getCacheKey()
    ));
  }

  @Test
  public void testRectangularBound()
  {
    float[][] insidePoints = new float[][]{
        {37.795717853074635f, -122.40906979480418f},
        {37.79625791859653f, -122.39638788940042f},
        {37.79685798676811f, -122.39335030726777f},
        {37.7966179600844f, -122.39798262002006f}
    };
    float[][] outsidePoints = new float[][]{
        {37.79805810848854f, -122.39236309307468f},
        {37.78197485768925f, -122.41886599718191f},
        {37.798298130492945f, -122.39608413118715f},
        {37.783595343766216f, -122.41932163450181f}
    };
    RectangularBound rectangularBound = new RectangularBound(
        new float[]{37.78185482027019f, -122.41795472254213f},
        new float[]{37.797638168104185f, -122.39228715352137f},
        10
    );
    for (float[] insidePoint : insidePoints) {
      Assert.assertTrue(rectangularBound.contains(insidePoint));
    }
    for (float[] outsidePoint : outsidePoints) {
      Assert.assertFalse(rectangularBound.contains(outsidePoint));
    }
  }

  @Test
  public void testDeSer() throws JsonProcessingException
  {
    Bound rectangularBound = new RectangularBound(
        new float[]{39.094969f, -84.516996f},
        new float[]{39.095473f, -84.515373f}
    );
    DefaultObjectMapper objectMapper = DefaultObjectMapper.INSTANCE;
    String val = objectMapper.writeValueAsString(rectangularBound);
    Bound deSerVal = objectMapper.readValue(val, Bound.class);
    Assert.assertEquals(deSerVal, rectangularBound);
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(RectangularBound.class)
                  .usingGetClass()
                  .verify();
  }
}
