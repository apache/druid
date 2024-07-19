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
import org.apache.druid.annotations.SubclassesMustOverrideEqualsAndHashCode;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "rows", value = WindowFrame.Rows.class),
    @JsonSubTypes.Type(name = "groups", value = WindowFrame.Groups.class),
})
@SubclassesMustOverrideEqualsAndHashCode
public interface WindowFrame
{
  static WindowFrame unbounded()
  {
    return rows(null, null);
  }

  static Rows rows(Integer lowerOffset, Integer upperOffset)
  {
    return new WindowFrame.Rows(lowerOffset, upperOffset);
  }

  static Groups groups(Integer lowerOffset, Integer upperOffset, List<String> orderByColumns)
  {
    return new WindowFrame.Groups(lowerOffset, upperOffset, orderByColumns);
  }

  static WindowFrame forOrderBy(String... orderByColumns)
  {
    return groups(null, 0, Lists.newArrayList(orderByColumns));
  }

  abstract class OffsetFrame implements WindowFrame
  {
    @JsonProperty
    public final Integer lowerOffset;
    @JsonProperty
    public final Integer upperOffset;

    @JsonCreator
    public OffsetFrame(
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

    /**
     * Calculates the applicable lower offset if the max number of rows is
     * known.
     */
    public int getLowerOffsetClamped(int maxRows)
    {
      if (lowerOffset == null) {
        return -maxRows;
      }
      return Math.max(-maxRows, lowerOffset);
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
      OffsetFrame other = (OffsetFrame) obj;
      return Objects.equals(lowerOffset, other.lowerOffset) && Objects.equals(upperOffset, other.upperOffset);
    }

    @Override
    public abstract String toString();
  }

  class Rows extends OffsetFrame
  {
    @JsonCreator
    public Rows(
        @JsonProperty("lowerOffset") Integer lowerOffset,
        @JsonProperty("upperOffset") Integer upperOffset)
    {
      super(lowerOffset, upperOffset);
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

  class Groups extends OffsetFrame
  {
    @JsonProperty
    private final ImmutableList<String> orderByColumns;

    @JsonCreator
    public Groups(
        @JsonProperty("lowerOffset") Integer lowerOffset,
        @JsonProperty("upperOffset") Integer upperOffset,
        @JsonProperty("orderByColumns") List<String> orderByColumns)
    {
      super(lowerOffset, upperOffset);
      this.orderByColumns = ImmutableList.copyOf(orderByColumns);
    }

    public List<String> getOrderByColumns()
    {
      return orderByColumns;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(lowerOffset, orderByColumns, upperOffset);
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
          && Objects.equals(orderByColumns, other.orderByColumns)
          && Objects.equals(upperOffset, other.upperOffset);
    }

    @Override
    public String toString()
    {
      return "WindowFrame.Groups [" +
          "lowerOffset=" + lowerOffset +
          ", upperOffset=" + upperOffset +
          ", orderByColumns=" + orderByColumns + "]";
    }
  }

  @SuppressWarnings("unchecked")
  @Nullable
  default <T extends WindowFrame> T unwrap(Class<T> clazz)
  {
    if (clazz.isInstance(this)) {
      return (T) this;
    }
    return null;
  }
}
