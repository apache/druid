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

package org.apache.druid.query.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQuery.OrderBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ColumnWithDirection
{
  public static ColumnWithDirection ascending(String column)
  {
    return new ColumnWithDirection(column, Direction.ASC);
  }

  public static ColumnWithDirection descending(String column)
  {
    return new ColumnWithDirection(column, Direction.DESC);
  }

  public static ArrayList<ColumnWithDirection> fromOrderBys(List<OrderBy> orderBys)
  {
    ArrayList<ColumnWithDirection> ordering = new ArrayList<>();
    for (ScanQuery.OrderBy orderBy : orderBys) {
      ordering.add(fromOrderBy(orderBy));
    }
    return ordering;
  }

  public static ArrayList<ColumnWithDirection> fromOrderBysColumnSpecs(List<OrderByColumnSpec> orderBySpecs)
  {
    ArrayList<ColumnWithDirection> ordering = new ArrayList<>();
    for (OrderByColumnSpec orderBySpec : orderBySpecs) {
      ordering.add(fromOrderBysColumnSpec(orderBySpec));
    }
    return ordering;
  }

  public static ColumnWithDirection fromOrderBysColumnSpec(OrderByColumnSpec orderBySpec)
  {
    return new ColumnWithDirection(orderBySpec.getDimension(),
        orderBySpec.getDirection() == OrderByColumnSpec.Direction.ASCENDING ? ColumnWithDirection.Direction.ASC
            : ColumnWithDirection.Direction.DESC);

  }

  public static ColumnWithDirection fromOrderBy(ScanQuery.OrderBy orderBy)
  {
    return new ColumnWithDirection(
        orderBy.getColumnName(),
        ScanQuery.Order.DESCENDING == orderBy.getOrder()
            ? ColumnWithDirection.Direction.DESC
            : ColumnWithDirection.Direction.ASC);
  }

  public enum Direction
  {
    ASC(1),
    DESC(-1);

    private final int directionInt;

    Direction(int directionInt)
    {
      this.directionInt = directionInt;
    }

    public int getDirectionInt()
    {
      return directionInt;
    }
  }

  private final String columnName;
  private final Direction direction;

  @JsonCreator
  public ColumnWithDirection(
      @JsonProperty("column") String columnName,
      @JsonProperty("direction") Direction direction
  )
  {
    this.columnName = columnName;
    this.direction = direction;
  }

  @JsonProperty("column")
  public String getColumn()
  {
    return columnName;
  }

  @JsonProperty("direction")
  public Direction getDirection()
  {
    return direction;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ColumnWithDirection)) {
      return false;
    }
    ColumnWithDirection that = (ColumnWithDirection) o;
    return Objects.equals(columnName, that.columnName) && direction == that.direction;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnName, direction);
  }

  @Override
  public String toString()
  {
    return "ColumnWithDirection{" +
           "columnName='" + columnName + '\'' +
           ", direction=" + direction +
           '}';
  }

}
