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

import com.google.common.base.Preconditions;

/**
 */
public class RTreeUtils
{

  public static double getEnclosingArea(Node a, Node b)
  {
    Preconditions.checkArgument(a.getNumDims() == b.getNumDims());

    double[] minCoords = new double[a.getNumDims()];
    double[] maxCoords = new double[a.getNumDims()];

    for (int i = 0; i < minCoords.length; i++) {
      minCoords[i] = Math.min(a.getMinCoordinates()[i], b.getMinCoordinates()[i]);
      maxCoords[i] = Math.max(a.getMaxCoordinates()[i], b.getMaxCoordinates()[i]);
    }

    double area = 1.0;
    for (int i = 0; i < minCoords.length; i++) {
      area *= (maxCoords[i] - minCoords[i]);
    }

    return area;
  }

  public static double getExpansionCost(Node node, Point point)
  {
    Preconditions.checkArgument(node.getNumDims() == point.getNumDims());

    if (node.contains(point.getCoords())) {
      return 0;
    }

    double expanded = 1.0;
    for (int i = 0; i < node.getNumDims(); i++) {
      double min = Math.min(point.getCoords()[i], node.getMinCoordinates()[i]);
      double max = Math.max(point.getCoords()[i], node.getMinCoordinates()[i]);
      expanded *= (max - min);
    }

    return (expanded - node.getArea());
  }

  public static void enclose(Node[] nodes)
  {
    for (Node node : nodes) {
      node.enclose();
    }
  }

  /**
   * Returns distance between two geo coordinates in meters according to https://en.wikipedia.org/wiki/Haversine_formula
   */
  public static double calculateHaversineDistance(
      final double lat1,
      final double lon1,
      final double lat2,
      final double lon2
  )
  {
    // Convert degrees to radians
    double radLat1 = Math.toRadians(lat1);
    double radLon1 = Math.toRadians(lon1);
    double radLat2 = Math.toRadians(lat2);
    double radLon2 = Math.toRadians(lon2);

    // Haversine formula
    double dLat = radLat2 - radLat1;
    double dLon = radLon2 - radLon1;

    double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
               Math.cos(radLat1) * Math.cos(radLat2) *
               Math.sin(dLon / 2) * Math.sin(dLon / 2);

    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    // Radius of Earth in meters (use 6371e3 for kilometers)
    double radius = 6371000.0;

    return radius * c;
  }

}
