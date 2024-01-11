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

package org.apache.druid.query.operator.window;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.query.operator.ColumnWithDirection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "unbounded", value = WindowFrame.Unbounded.class),
    @JsonSubTypes.Type(name = "rows", value = WindowFrame.Rows.class),
    @JsonSubTypes.Type(name = "group", value = WindowFrame.Groups.class),
})
public interface WindowFrame extends Adaptable
{
  public static WindowFrame unbounded()
  {
    return new WindowFrame.Unbounded();
  }

  public static Rows rows(Integer lowerOffset, Integer upperOffset)
  {
    return new WindowFrame.Rows(lowerOffset, upperOffset);
  }

  public static Groups groups(
      final Integer lowerOffset,
      final Integer upperOffset,
      final List<ColumnWithDirection> orderBy)
  {
    return new WindowFrame.Groups(lowerOffset, upperOffset, orderBy);

  }

  public static WindowFrame forOrderBy(ColumnWithDirection... orderBy)
  {
    return groups(null, 0, Lists.newArrayList(orderBy));
  }

  @Deprecated
  @SuppressWarnings("unused")
  public enum PeerType
  {
    ROWS, RANGE1
  }

  public static class Unbounded implements WindowFrame
  {
    @JsonCreator
    public Unbounded()
    {
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      return getClass() == obj.getClass();
    }

    @Override
    public int hashCode()
    {
      return 0;
    }

    @Override
    public String toString()
    {
      return "WindowFrame.Unbounded []";
    }
  }

  public static class Rows implements WindowFrame
  {
    @JsonProperty
    public Integer lowerOffset;
    @JsonProperty
    public Integer upperOffset;

    @JsonCreator
    public Rows(
        @JsonProperty("lowerOffset") Integer lowerOffset,
        @JsonProperty("upperOffset") Integer upperOffset)
    {
      this.lowerOffset = lowerOffset;
      this.upperOffset = upperOffset;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(lowerOffset, upperOffset);
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Rows other = (Rows) obj;
      return Objects.equals(lowerOffset, other.lowerOffset) && Objects.equals(upperOffset, other.upperOffset);
    }

    @Override
    public String toString()
    {
      return "WindowFrame.Rows ["
          + "lowerOffset=" + lowerOffset +
          ", upperOffset=" + upperOffset +
          "]";
    }
  }

  public static class Groups implements WindowFrame
  {
    @JsonProperty
    private final Integer lowerOffset;
    @JsonProperty
    private final Integer upperOffset;
    @JsonProperty
    private final ImmutableList<ColumnWithDirection> orderBy;

    @JsonCreator
    public Groups(
        @JsonProperty("lowerOffset") Integer lowerOffset,
        @JsonProperty("upperOffset") Integer upperOffset,
        @JsonProperty("orderBy") List<ColumnWithDirection> orderBy)
    {
      this.lowerOffset = lowerOffset;
      this.upperOffset = upperOffset;
      this.orderBy = ImmutableList.copyOf(orderBy);
    }

    public List<String> getOrderByColNames()
    {
      if (orderBy == null) {
        return Collections.emptyList();
      }
      return orderBy.stream().map(ColumnWithDirection::getColumn).collect(Collectors.toList());
    }

    /**
     * Calculates the applicable lower offset if the max number of rows is
     * known.
     */
    public int getLowerOffsetClamped(int maxRows)
    {
      if (lowerOffset == null) {
        return maxRows;
      }
      return Math.min(maxRows, lowerOffset);
    }

    /**
     * Calculates the applicable upper offset if the max number of rows is
     * known.
     */
    public int getUpperOffsetClamped(int maxRows)
    {
      if (upperOffset == null) {
        return maxRows;
      }
      return Math.min(maxRows, upperOffset);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(lowerOffset, orderBy, upperOffset);
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Groups other = (Groups) obj;
      return Objects.equals(lowerOffset, other.lowerOffset)
          && Objects.equals(orderBy, other.orderBy)
          && Objects.equals(upperOffset, other.upperOffset);
    }

    @Override
    public String toString()
    {
      return "Groups [" +
          "lowerOffset=" + lowerOffset +
          ", upperOffset=" + upperOffset +
          ", orderBy=" + orderBy + "]";
    }

  }
}
