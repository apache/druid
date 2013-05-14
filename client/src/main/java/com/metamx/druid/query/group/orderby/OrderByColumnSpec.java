package com.metamx.druid.query.group.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import com.metamx.common.ISE;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class OrderByColumnSpec
{
  /**
   * Maintain a map of the enum values so that we can just do a lookup and get a null if it doesn't exist instead
   * of an exception thrown.
   */
  private static final Map<String, Direction> stupidEnumMap;

  static
  {
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
    }
    else if (obj instanceof Map) {
      final Map map = (Map) obj;

      final String dimension = map.get("dimension").toString();
      final Direction direction = determineDirection(map.get("direction"));

      return new OrderByColumnSpec(dimension, direction);
    }
    else {
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
  public String toString()
  {
    return "OrderByColumnSpec{" +
           "dimension='" + dimension + '\'' +
           ", direction=" + direction +
           '}';
  }

  public static enum Direction
  {
    ASCENDING,
    DESCENDING
  }
}
