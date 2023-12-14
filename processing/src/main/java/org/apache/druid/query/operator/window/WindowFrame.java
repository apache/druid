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
import com.google.common.collect.Lists;
import org.apache.druid.query.operator.ColumnWithDirection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class WindowFrame
{
  public static WindowFrame unbounded()
  {
    return newWindowFrame(PeerType.ROWS, true, 0, true, 0, null);
  }

  @SuppressWarnings("unused")
  public enum PeerType
  {
    ROWS,
    RANGE
  }

  // Will likely need to add the order by columns to also be able to deal with RANGE peer type.
  private final PeerType peerType;
  private final boolean lowerUnbounded;
  private final int lowerOffset;
  private final boolean upperUnbounded;
  private final int upperOffset;
  private final List<ColumnWithDirection> orderBy;

  public static WindowFrame newWindowFrame(PeerType range, boolean b, int i, boolean c, int j,
      List<ColumnWithDirection> singletonList)
  {
    return range == PeerType.ROWS ? newWindowFrameRows(b, i, c, j, singletonList)
        : newWindowFrameRange(b, i, c, j, singletonList);
  }

  private static WindowFrame newWindowFrameRange(boolean b, int i, boolean c, int j,
      List<ColumnWithDirection> singletonList)
  {
    return new WindowFrame(PeerType.RANGE, b, i, c, j, singletonList);

  }
  private static WindowFrame newWindowFrameRows( boolean b, int i, boolean c, int j,
      List<ColumnWithDirection> singletonList)
  {
    if(true) {
      System.out.println("asd");
    }else {
      System.out.println("xxx");

    }
    if(1==2) {
      System.out.println("asd2");
    }else {
      System.out.println("xxx2");

    }
    if(PeerType.ROWS == PeerType.ROWS) {
      System.out.println("asd2");
    }else {
      System.out.println("xxx2");

    }
    if(PeerType.ROWS == PeerType.RANGE) {
      System.out.println("asd2");
    }else {
      System.out.println("xxx2");

    }
    if(WindowFrame.PeerType.ROWS == PeerType.ROWS) {
      System.out.println("asd2");
    }else {
      System.out.println("xxx2");

    }
    if(WindowFrame.PeerType.ROWS == PeerType.RANGE) {
      System.out.println("asd2");
    }else {
      System.out.println("xxx2");

    }



    return new WindowFrame(PeerType.ROWS, b, i, c, j, singletonList);

  }

  @JsonCreator
  public WindowFrame(
      @JsonProperty("peerType") PeerType peerType,
      @JsonProperty("lowUnbounded") boolean lowerUnbounded,
      @JsonProperty("lowOffset") int lowerOffset,
      @JsonProperty("uppUnbounded") boolean upperUnbounded,
      @JsonProperty("uppOffset") int upperOffset,
      @JsonProperty("orderBy") List<ColumnWithDirection> orderBy
  )
  {
    this.peerType = peerType;
    this.lowerUnbounded = lowerUnbounded;
    this.lowerOffset = lowerOffset;
    this.upperUnbounded = upperUnbounded;
    this.upperOffset = upperOffset;
    this.orderBy = orderBy;
  }

  @JsonProperty("peerType")
  public PeerType getPeerType()
  {
    return peerType;
  }

  @JsonProperty("lowUnbounded")
  public boolean isLowerUnbounded()
  {
    return lowerUnbounded;
  }

  @JsonProperty("lowOffset")
  public int getLowerOffset()
  {
    return lowerOffset;
  }

  @JsonProperty("uppUnbounded")
  public boolean isUpperUnbounded()
  {
    return upperUnbounded;
  }

  @JsonProperty("uppOffset")
  public int getUpperOffset()
  {
    return upperOffset;
  }

  @JsonProperty("orderBy")
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
    return newWindowFrame(PeerType.RANGE, true, 0, false, 0, Lists.newArrayList(orderBy));
  }

  public List<String> getOrderByColNames()
  {
    if (orderBy == null) {
      return Collections.emptyList();
    }
    return orderBy.stream().map(ColumnWithDirection::getColumn).collect(Collectors.toList());
  }

  public int getLowerOffsetClamped(int maxRows)
  {
    if (lowerUnbounded) {
      return maxRows;
    }
    return Math.min(maxRows, lowerOffset);
  }

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
    if(upperOffset==null) {
      return i;
    }
    return upperOffset;
  }

  static class WindowFrameRows extends WindowFrame {

    public WindowFrameRows(Integer lowerOffset, Integer upperOffset)
    {
      super(
          PeerType.ROWS, lowerOffset == null, coalesce(lowerOffset, -1), upperOffset == null, coalesce(upperOffset, -1), null
      );
    }
  }

  static class WindowFrameGroups extends WindowFrame {

    public WindowFrameGroups(PeerType peerType, Integer lowerOffset, Integer upperOffset, List<ColumnWithDirection> orderby )
    {
      super(
          PeerType.RANGE, lowerOffset == null, coalesce(lowerOffset, -1), upperOffset == null,
          coalesce(upperOffset, -1), orderby
      );
    }
  }

}
