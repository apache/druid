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
import org.apache.druid.collections.spatial.ImmutablePoint;

import java.nio.ByteBuffer;

/**
 */
public class RadiusBound extends RectangularBound
{
  private static final byte CACHE_TYPE_ID = 0x01;
  private final float[] coords;
  private final float radius;

  @JsonCreator
  public RadiusBound(
      @JsonProperty("coords") float[] coords,
      @JsonProperty("radius") float radius,
      @JsonProperty("limit") int limit
  )
  {
    super(getMinCoords(coords, radius), getMaxCoords(coords, radius), limit);

    this.coords = coords;
    this.radius = radius;
  }

  public RadiusBound(
      float[] coords,
      float radius
  )
  {
    this(coords, radius, 0);
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

  @Override
  public boolean contains(float[] otherCoords)
  {
    double total = 0.0;
    for (int i = 0; i < coords.length; i++) {
      total += Math.pow(otherCoords[i] - coords[i], 2);
    }

    return (total <= Math.pow(radius, 2));
  }

  @Override
  public Iterable<ImmutablePoint> filter(Iterable<ImmutablePoint> points)
  {
    return Iterables.filter(
        points,
        new Predicate<ImmutablePoint>()
        {
          @Override
          public boolean apply(ImmutablePoint point)
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
}
