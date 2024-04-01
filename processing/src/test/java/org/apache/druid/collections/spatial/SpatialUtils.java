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

package org.apache.druid.collections.spatial;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SpatialUtils
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
}
