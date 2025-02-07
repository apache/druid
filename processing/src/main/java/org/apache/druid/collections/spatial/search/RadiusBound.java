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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.druid.collections.spatial.ImmutableFloatPoint;
import org.apache.druid.collections.spatial.RTreeUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 *
 */
public class RadiusBound extends RectangularBound
{
  private static final byte CACHE_TYPE_ID = 0x01;
  private final float[] coords;
  private final float radius;
  private final RadiusUnit radiusUnit;

  @JsonCreator
  public RadiusBound(
      @JsonProperty("coords") float[] coords,
      @JsonProperty("radius") float radius,
      @JsonProperty("limit") int limit,
      @JsonProperty("radiusUnit") @Nullable RadiusUnit radiusUnit
  )
  {
    super(getMinCoords(coords, radius), getMaxCoords(coords, radius), limit);

    this.coords = coords;
    this.radius = radius;
    this.radiusUnit = radiusUnit == null ? RadiusUnit.euclidean : radiusUnit;
  }

  public RadiusBound(
      float[] coords,
      float radius,
      int limit
  )
  {
    this(coords, radius, limit, null);
  }

  public RadiusBound(
      float[] coords,
      float radius,
      RadiusUnit radiusUnit
  )
  {
    this(coords, radius, 0, radiusUnit);
  }

  public RadiusBound(
      float[] coords,
      float radius
  )
  {
    this(coords, radius, 0, null);
  }

  private static float[] getMinCoords(float[] coords, float radius)
  {
    float[] retVal = new float[coords.length];
    for (int i = 0; i < coords.length; i++) {
      retVal[i] = coords[i] - radius;
    }
    return retVal;
  }

  private static float[] getMaxCoords(float[] coords, float radius)
  {
    float[] retVal = new float[coords.length];
    for (int i = 0; i < coords.length; i++) {
      retVal[i] = coords[i] + radius;
    }
    return retVal;
  }

  @JsonProperty
  public float[] getCoords()
  {
    return coords;
  }

  @JsonProperty
  public float getRadius()
  {
    return radius;
  }

  @JsonProperty
  public RadiusUnit getRadiusUnit()
  {
    return radiusUnit;
  }

  @Override
  public boolean contains(float[] otherCoords)
  {
    if (otherCoords.length < 2 || coords.length < 2) {
      return false;
    }
    if (radiusUnit == RadiusUnit.euclidean) {
      double total = 0.0;
      for (int i = 0; i < coords.length; i++) {
        total += Math.pow(otherCoords[i] - coords[i], 2);
      }
      return (total <= Math.pow(radius, 2));
    } else {
      double radiusInMeters = getRadius() * radiusUnit.getMetersMultiFactor();
      double distance = RTreeUtils.calculateHaversineDistance(coords[0], coords[1], otherCoords[0], otherCoords[1]);
      return distance <= radiusInMeters;
    }
  }

  @Override
  public Iterable<ImmutableFloatPoint> filter(Iterable<ImmutableFloatPoint> points)
  {
    return Iterables.filter(
        points,
        new Predicate<>()
        {
          @Override
          public boolean apply(ImmutableFloatPoint point)
          {
            return contains(point.getCoords());
          }
        }
    );
  }

  @Override
  public byte[] getCacheKey()
  {
    final ByteBuffer minCoordsBuffer = ByteBuffer.allocate(coords.length * Float.BYTES);
    minCoordsBuffer.asFloatBuffer().put(coords);
    final byte[] minCoordsCacheKey = minCoordsBuffer.array();
    final ByteBuffer cacheKey = ByteBuffer
        .allocate(1 + minCoordsCacheKey.length + Integer.BYTES + Float.BYTES)
        .put(minCoordsCacheKey)
        .putFloat(radius)
        .putInt(getLimit())
        .put(CACHE_TYPE_ID);
    return cacheKey.array();
  }

  public enum RadiusUnit
  {
    meters(1),
    euclidean(1),
    @SuppressWarnings("unused") // will be used in high precision filtering
    miles(1609.344f),
    @SuppressWarnings("unused")
    kilometers(1000);

    float metersMultiFactor;

    RadiusUnit(float mmf)
    {
      this.metersMultiFactor = mmf;
    }

    public float getMetersMultiFactor()
    {
      return metersMultiFactor;
    }
  }
}
