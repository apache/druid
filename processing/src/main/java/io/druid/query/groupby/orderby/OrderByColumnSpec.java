/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import com.metamx.common.ISE;

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

  private final boolean asNumber;
  private final String dimension;
  private final Direction direction;

  @JsonCreator
  public static OrderByColumnSpec create(Object obj)
  {
    Preconditions.checkNotNull(obj, "Cannot build an OrderByColumnSpec from a null object.");

    if (obj instanceof String) {
      return new OrderByColumnSpec(false, obj.toString(), null);
    } else if (obj instanceof Map) {
      final Map map = (Map) obj;

      final boolean asNumber;
      if (map.get("asNumber") instanceof Boolean) {
        asNumber = (Boolean) map.get("asNumber");
      } else {
        asNumber = false;
      }
      final String dimension = map.get("dimension").toString();
      final Direction direction = determineDirection(map.get("direction"));

      return new OrderByColumnSpec(asNumber, dimension, direction);
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
    this(false, dimension, direction);
  }

  public OrderByColumnSpec(
      boolean asNumber,
      String dimension,
      Direction direction
  )
  {
    this.asNumber = asNumber;
    this.dimension = dimension;
    this.direction = direction == null ? Direction.ASCENDING : direction;
  }

  @JsonProperty
  public boolean asNumber()
  {
    return asNumber;
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
  public String toString()
  {
    return "OrderByColumnSpec{" +
           "dimension='" + dimension + '\'' +
           ", direction=" + direction +
           '}';
  }

  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = dimension.getBytes(Charsets.UTF_8);
    final byte[] directionBytes = direction.name().getBytes(Charsets.UTF_8);

    return ByteBuffer.allocate(dimensionBytes.length + directionBytes.length)
                     .put(dimensionBytes)
                     .put(directionBytes)
                     .array();
  }
}
