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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class OrderByColumnSpec
{
  public static enum Direction
  {
    ASCENDING,
    DESCENDING
  }

  /**
   * Maintain a map of the enum values so that we can just do a lookup and get a null if it doesn't exist instead
   * of an exception thrown.
   */
  private static final Map<String, Direction> stupidEnumMap;

  static {
    final ImmutableMap.Builder<String, Direction> bob = ImmutableMap.builder();
    for (Direction direction : Direction.values()) {
      bob.put(direction.toString(), direction);
    }
    stupidEnumMap = bob.build();
  }

  private final String dimension;
  private final Direction direction;

  @JsonCreator
  public static OrderByColumnSpec create(Object obj)
  {
    Preconditions.checkNotNull(obj, "Cannot build an OrderByColumnSpec from a null object.");

    if (obj instanceof String) {
      return new OrderByColumnSpec(obj.toString(), null);
    } else if (obj instanceof Map) {
      final Map map = (Map) obj;

      final String dimension = map.get("dimension").toString();
      final Direction direction = determineDirection(map.get("direction"));

      return new OrderByColumnSpec(dimension, direction);
    } else {
      throw new ISE("Cannot build an OrderByColumnSpec from a %s", obj.getClass());
    }
  }

  public static OrderByColumnSpec asc(String dimension)
  {
    return new OrderByColumnSpec(dimension, Direction.ASCENDING);
  }

  public static List<OrderByColumnSpec> ascending(String... dimension)
  {
    return Lists.transform(
        Arrays.asList(dimension),
        new Function<String, OrderByColumnSpec>()
        {
          @Override
          public OrderByColumnSpec apply(@Nullable String input)
          {
            return asc(input);
          }
        }
    );
  }

  public static OrderByColumnSpec desc(String dimension)
  {
    return new OrderByColumnSpec(dimension, Direction.DESCENDING);
  }

  public static List<OrderByColumnSpec> descending(String... dimension)
  {
    return Lists.transform(
        Arrays.asList(dimension),
        new Function<String, OrderByColumnSpec>()
        {
          @Override
          public OrderByColumnSpec apply(@Nullable String input)
          {
            return desc(input);
          }
        }
    );
  }

  public OrderByColumnSpec(
      String dimension,
      Direction direction
  )
  {
    this.dimension = dimension;
    this.direction = direction == null ? Direction.ASCENDING : direction;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public Direction getDirection()
  {
    return direction;
  }

  public static Direction determineDirection(Object directionObj)
  {
    if (directionObj == null) {
      return null;
    }

    String directionString = directionObj.toString();

    Direction direction = stupidEnumMap.get(directionString);

    if (direction == null) {
      final String lowerDimension = directionString.toLowerCase();

      for (Direction dir : Direction.values()) {
        if (dir.toString().toLowerCase().startsWith(lowerDimension)) {
          if (direction != null) {
            throw new ISE("Ambiguous directions[%s] and [%s]", direction, dir);
          }
          direction = dir;
        }
      }
    }

    if (direction == null) {
      throw new IAE("Unknown direction[%s]", directionString);
    }

    return direction;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OrderByColumnSpec that = (OrderByColumnSpec) o;

    if (!dimension.equals(that.dimension)) {
      return false;
    }
    return direction == that.direction;

  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + direction.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "OrderByColumnSpec{" +
           "dimension='" + dimension + '\'' +
           ", direction=" + direction +
           '}';
  }

  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    final byte[] directionBytes = StringUtils.toUtf8(direction.name());

    return ByteBuffer.allocate(dimensionBytes.length + directionBytes.length)
                     .put(dimensionBytes)
                     .put(directionBytes)
                     .array();
  }
}
