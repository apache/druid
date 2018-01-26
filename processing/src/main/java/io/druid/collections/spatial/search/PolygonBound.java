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

package io.druid.collections.spatial.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import io.druid.collections.spatial.ImmutablePoint;

import java.nio.ByteBuffer;

/**
 */
public class PolygonBound extends RectangularBound
{
  private static final byte CACHE_TYPE_ID = 0x02;

  private final float[] abscissa;
  private final float[] ordinate;

  private PolygonBound(float[] abscissa, float[] ordinate, int limit)
  {
    super(getMinCoords(abscissa, ordinate), getMaxCoords(abscissa, ordinate), limit);
    this.abscissa = abscissa;
    this.ordinate = ordinate;
  }

  private static float[] getMinCoords(float[] abscissa, float[] ordinate)
  {
    float[] retVal = new float[2];
    retVal[0] = abscissa[0];
    retVal[1] = ordinate[0];

    for (int i = 1; i < abscissa.length; i++) {
      if (abscissa[i] < retVal[0]) {
        retVal[0] = abscissa[i];
      }
      if (ordinate[i] < retVal[1]) {
        retVal[1] = ordinate[i];
      }
    }
    return retVal;
  }

  private static float[] getMaxCoords(float[] abscissa, float[] ordinate)
  {
    float[] retVal = new float[2];
    retVal[0] = abscissa[0];
    retVal[1] = ordinate[0];
    for (int i = 1; i < abscissa.length; i++) {
      if (abscissa[i] > retVal[0]) {
        retVal[0] = abscissa[i];
      }
      if (ordinate[i] > retVal[1]) {
        retVal[1] = ordinate[i];
      }
    }
    return retVal;
  }

  /**
   * abscissa and ordinate contain the coordinates of polygon.
   * abscissa[i] is the horizontal coordinate for the i'th corner of the polygon,
   * and ordinate[i] is the vertical coordinate for the i'th corner.
   * The polygon must have more than 2 corners, so the length of abscissa or ordinate must be equal or greater than 3.
   *
   * if the polygon is a rectangular, which corners are {0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0},
   * the abscissa should be {0.0, 0.0, 1.0, 1.0} and ordinate should be {0.0, 1.0, 1.0, 0.0}
   */
  @JsonCreator
  public static PolygonBound from(
      @JsonProperty("abscissa") float[] abscissa,
      @JsonProperty("ordinate") float[] ordinate,
      @JsonProperty("limit") int limit
  )
  {
    Preconditions.checkArgument(abscissa.length == ordinate.length); //abscissa and ordinate should be the same length
    Preconditions.checkArgument(abscissa.length > 2); //a polygon should have more than 2 corners
    return new PolygonBound(abscissa, ordinate, limit);
  }

  public static PolygonBound from(float[] abscissa, float[] ordinate)
  {
    return PolygonBound.from(abscissa, ordinate, 0);
  }

  @JsonProperty
  public float[] getOrdinate()
  {
    return ordinate;
  }

  @JsonProperty
  public float[] getAbscissa()
  {
    return abscissa;
  }

  @Override
  public boolean contains(float[] coords)
  {
    int polyCorners = abscissa.length;
    int j = polyCorners - 1;
    boolean oddNodes = false;
    for (int i = 0; i < polyCorners; i++) {

      if (abscissa[i] == coords[0] && ordinate[i] == coords[1]) {
        return true;
      }

      if (isPointLayingOnHorizontalBound(i, j, coords)) {
        return true;
      }

      if (between(ordinate[i], ordinate[j], coords[1]) && (abscissa[j] <= coords[0] || abscissa[i] <= coords[0])) {
        float intersectionPointX = abscissa[i] + (coords[1] - ordinate[i]) / (ordinate[j] - ordinate[i]) * (abscissa[j] - abscissa[i]);

        if (intersectionPointX == coords[0]) {
          return true;
        } else if (intersectionPointX < coords[0]) {
          oddNodes = !oddNodes;
        }
      }
      j = i;
    }
    return oddNodes;
  }

  private boolean isPointLayingOnHorizontalBound(int i, int j, float[] coords)
  {
    return ordinate[i] == ordinate[j] && ordinate[j] == coords[1] && between(abscissa[i], abscissa[j], coords[0]);
  }

  private static boolean between(float a, float b, float x)
  {
    if (a <= b) {
      return a <= x && x <= b;
    } else {
      return b <= x && x <= a;
    }
  }

  @Override
  public Iterable<ImmutablePoint> filter(Iterable<ImmutablePoint> points)
  {
    return Iterables.filter(
        points,
        new Predicate<ImmutablePoint>()
        {
          @Override
          public boolean apply(ImmutablePoint immutablePoint)
          {
            return contains(immutablePoint.getCoords());
          }
        }
    );
  }

  @Override
  public byte[] getCacheKey()
  {
    ByteBuffer abscissaBuffer = ByteBuffer.allocate(abscissa.length * Floats.BYTES);
    abscissaBuffer.asFloatBuffer().put(abscissa);
    final byte[] abscissaCacheKey = abscissaBuffer.array();

    ByteBuffer ordinateBuffer = ByteBuffer.allocate(ordinate.length * Floats.BYTES);
    ordinateBuffer.asFloatBuffer().put(ordinate);
    final byte[] ordinateCacheKey = ordinateBuffer.array();

    final ByteBuffer cacheKey = ByteBuffer
        .allocate(1 + abscissaCacheKey.length + ordinateCacheKey.length + Ints.BYTES)
        .put(abscissaCacheKey)
        .put(ordinateCacheKey)
        .putInt(getLimit())
        .put(CACHE_TYPE_ID);

    return cacheKey.array();
  }
}
