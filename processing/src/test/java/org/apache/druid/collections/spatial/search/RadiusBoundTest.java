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
import org.apache.druid.collections.spatial.RTreeUtils;
import org.apache.druid.collections.spatial.SpatialUtils;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class RadiusBoundTest
{
  @Test
  public void testCacheKey()
  {
    final float[] coords0 = new float[]{1.0F, 2.0F};
    final float[] coords1 = new float[]{1.1F, 2.1F};
    Assert.assertArrayEquals(
        new RadiusBound(coords0, 3.0F, 10).getCacheKey(),
        new RadiusBound(coords0, 3.0F, 10).getCacheKey()
    );
    Assert.assertFalse(Arrays.equals(
        new RadiusBound(coords0, 3.0F, 10).getCacheKey(),
        new RadiusBound(coords1, 3.0F, 10).getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        new RadiusBound(coords0, 3.0F, 10).getCacheKey(),
        new RadiusBound(coords0, 3.1F, 10).getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        new RadiusBound(coords0, 3.0F, 10).getCacheKey(),
        new RadiusBound(coords0, 3.0F, 9).getCacheKey()
    ));
  }

  @Test
  public void testContains()
  {
    float circleCenterLat = 12.3456789f;
    float circleCenterLon = 45.6789012f;
    float circleRadius = 500.0f; // Radius in meters
    int numberOfPoints = 1000;

    float[] center = new float[]{circleCenterLat, circleCenterLon};
    Bound bound = new RadiusBound(center, circleRadius, 100, RadiusBound.RadiusUnit.meters);

    float[][] geoInsidePoints = SpatialUtils.generateGeoCoordinatesAroundCircle(
        circleCenterLat,
        circleCenterLon,
        circleRadius,
        numberOfPoints,
        true
    );

    for (float[] geoPoint : geoInsidePoints) {
      double distance = RTreeUtils.calculateHaversineDistance(geoPoint[0], geoPoint[1], center[0], center[1]);
      Assert.assertTrue(distance < circleRadius);
      Assert.assertTrue(bound.contains(geoPoint));
      float[] floatPoint = new float[]{
          Float.parseFloat(String.valueOf(geoPoint[0])),
          Float.parseFloat(String.valueOf(geoPoint[1]))
      };
      Assert.assertTrue(bound.contains(floatPoint));
    }

    float[][] geoOutsidePoints = SpatialUtils.generateGeoCoordinatesAroundCircle(
        circleCenterLat,
        circleCenterLon,
        circleRadius,
        numberOfPoints,
        false
    );

    for (float[] geoPoint : geoOutsidePoints) {
      double haversineDistance = RTreeUtils.calculateHaversineDistance(geoPoint[0], geoPoint[1], center[0], center[1]);
      Assert.assertTrue(haversineDistance > circleRadius); // asserts that point is outside
      Assert.assertFalse(bound.contains(geoPoint));
      float[] floatPoint = new float[]{
          Float.parseFloat(String.valueOf(geoPoint[0])),
          Float.parseFloat(String.valueOf(geoPoint[1]))
      };
      Assert.assertFalse(bound.contains(floatPoint));
    }
  }

  @Test
  public void deSerTest() throws JsonProcessingException
  {
    float circleCenterLat = 12.3456789f;
    float circleCenterLon = 45.6789012f;
    float circleRadius = 500.0f; // Radius in meters

    float[] center = new float[]{circleCenterLat, circleCenterLon};
    Bound bound = new RadiusBound(center, circleRadius, 100);
    DefaultObjectMapper objectMapper = DefaultObjectMapper.INSTANCE;
    Bound val = objectMapper.readValue(objectMapper.writeValueAsString(bound), Bound.class);
    Assert.assertEquals(bound, val);

    Bound bound1 = new RadiusBound(center, circleRadius, 100, RadiusBound.RadiusUnit.meters);
    Bound val1 = objectMapper.readValue(objectMapper.writeValueAsString(bound1), Bound.class);
    Assert.assertEquals(bound1, val1);
  }
}
