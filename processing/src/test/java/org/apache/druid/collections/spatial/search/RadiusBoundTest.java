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

import org.apache.druid.collections.spatial.RTreeUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class RadiusBoundTest
{
  private static Random random = ThreadLocalRandom.current();

  public static float[][] generateGeoCoordinatesAroundCircle(
      float circleCenterLat,
      float circleCenterLon,
      float circleRadius,
      int numberOfPoints,
      boolean shouldBeInside
  )
  {
    float[][] geoCoordinates = new float[numberOfPoints][2];

    for (int i = 0; i < numberOfPoints; i++) {
      double angle = 2 * Math.PI * random.nextDouble();
      double distance;
      if (shouldBeInside) {
        // Generate random distance within the circle's radius
        distance = circleRadius * Math.sqrt(random.nextDouble()) - 1;
      } else {
        // Generate random points outside of circle but slightly beyond the circle's radius
        distance = circleRadius + 100 * random.nextDouble();
      }

      // Calculate new latitude and longitude
      double latitude = circleCenterLat
                        + distance * Math.cos(angle) / 111000; // 1 degree is approximately 111,000 meters
      double longitude = circleCenterLon + distance * Math.sin(angle) / (111000 * Math.cos(Math.toRadians(latitude)));

      geoCoordinates[i][0] = (float) latitude;
      geoCoordinates[i][1] = (float) longitude;
    }
    return geoCoordinates;
  }

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

    float[][] geoInsidePoints = generateGeoCoordinatesAroundCircle(
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

    float[][] geoOutsidePoints = generateGeoCoordinatesAroundCircle(
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
}
