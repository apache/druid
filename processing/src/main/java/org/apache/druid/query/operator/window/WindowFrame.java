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
public class WindowFrame implements Adaptable
{
  public static WindowFrame unbounded()
  {
    return new Unbounded();
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

  @Deprecated
  @SuppressWarnings("unused")
  public enum PeerType
  {
    ROWS, RANGE1
  }

  // Will likely need to add the order by columns to also be able to deal with
  // RANGE peer type.
  private final PeerType peerType;
  private final boolean lowerUnbounded;
  private final int lowerOffset;
  private final boolean upperUnbounded;
  private final int upperOffset;
  private final List<ColumnWithDirection> orderBy;

  public WindowFrame(
      PeerType peerType,
      boolean lowerUnbounded,
      int lowerOffset,
      boolean upperUnbounded,
      int upperOffset,
      List<ColumnWithDirection> orderBy)
  {
    this.peerType = peerType;
    this.lowerUnbounded = lowerUnbounded;
    this.lowerOffset = lowerOffset;
    this.upperUnbounded = upperUnbounded;
    this.upperOffset = upperOffset;
    this.orderBy = orderBy;
  }

  public PeerType getPeerType()
  {
    return peerType;
  }

  public boolean isLowerUnbounded()
  {
    return lowerUnbounded;
  }

  public int getLowerOffset()
  {
    return lowerOffset;
  }

  public boolean isUpperUnbounded()
  {
    return upperUnbounded;
  }

  public int getUpperOffset()
  {
    return upperOffset;
  }

  public List<ColumnWithDirection> getOrderBy()
  {
    return orderBy;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WindowFrame)) {
      return false;
    }
    WindowFrame that = (WindowFrame) o;
    return lowerUnbounded == that.lowerUnbounded
        && lowerOffset == that.lowerOffset
        && upperUnbounded == that.upperUnbounded
        && upperOffset == that.upperOffset
        && peerType == that.peerType
        && Objects.equals(orderBy, that.orderBy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(peerType, lowerUnbounded, lowerOffset, upperUnbounded, upperOffset, orderBy);
  }

  @Override
  public String toString()
  {
    return "WindowFrame{" +
        "peerType=" + peerType +
        ", lowerUnbounded=" + lowerUnbounded +
        ", lowerOffset=" + lowerOffset +
        ", upperUnbounded=" + upperUnbounded +
        ", upperOffset=" + upperOffset +
        ", orderBy=" + orderBy +
        '}';
  }

  public static WindowFrame forOrderBy(ColumnWithDirection... orderBy)
  {
    return groups(null, 0, Lists.newArrayList(orderBy));
  }

  public List<String> getOrderByColNames()
  {
    if (orderBy == null) {
      return Collections.emptyList();
    }
    return orderBy.stream().map(ColumnWithDirection::getColumn).collect(Collectors.toList());
  }

  /**
   * Calculates the applicable lower offset if the max number of rows is known.
   */
  public int getLowerOffsetClamped(int maxRows)
  {
    if (lowerUnbounded) {
      return maxRows;
    }
    return Math.min(maxRows, lowerOffset);
  }

  /**
   * Calculates the applicable upper offset if the max number of rows is known.
   */
  public int getUpperOffsetClamped(int maxRows)
  {
    if (upperUnbounded) {
      return maxRows;
    }
    return Math.min(maxRows, upperOffset);
  }

  @Deprecated
  private static int coalesce(Integer upperOffset, int i)
  {
    if (upperOffset == null) {
      return i;
    }
    return upperOffset;
  }

  public static class Rows extends WindowFrame
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
      super(
          PeerType.ROWS, lowerOffset == null, coalesce(lowerOffset, 0), upperOffset == null, coalesce(upperOffset, 0),
          null
      );
      this.lowerOffset = lowerOffset;
      this.upperOffset = upperOffset;
    }
  }

  public static class Unbounded extends WindowFrame
  {
    @JsonCreator
    public Unbounded()
    {
      super(null, false, 0, false, 0, null);
    }
  }

  public static class Groups extends WindowFrame
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
      super(
          null, lowerOffset == null, coalesce(lowerOffset, 0), upperOffset == null,
          coalesce(upperOffset, 0), orderBy
      );
      this.lowerOffset = lowerOffset;
      this.upperOffset = upperOffset;
      this.orderBy = ImmutableList.copyOf(orderBy);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getAdapter(Class<T> clazz)
  {
    if (clazz == getClass()) {
      return (T) this;
    }
    return null;
  }
}
