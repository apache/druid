/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query.group;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.ISE;
import com.metamx.druid.input.MapBasedRow;
import com.metamx.druid.input.Row;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

/**
 */
public class DefaultLimitSpec implements LimitSpec
{
  private static final byte CACHE_TYPE_ID = 0x0;
  private static Joiner JOINER = Joiner.on("");

  private final List<String> orderBy;
  private final int limit;
  private final Comparator<Row> comparator;

  @JsonCreator
  public DefaultLimitSpec(
      @JsonProperty("orderBy") List<String> orderBy,
      @JsonProperty("limit") int limit
  )
  {
    this.orderBy = (orderBy == null) ? Lists.<String>newArrayList() : orderBy;
    this.limit = limit;
    this.comparator = makeComparator();
  }

  public DefaultLimitSpec()
  {
    this.orderBy = Lists.newArrayList();
    this.limit = 0;
    this.comparator = makeComparator();
  }

  @JsonProperty
  @Override
  public List<String> getOrderBy()
  {
    return orderBy;
  }

  @JsonProperty
  @Override
  public int getLimit()
  {
    return limit;
  }

  @Override
  public Comparator<Row> getComparator()
  {
    return comparator;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] orderByBytes = JOINER.join(orderBy).getBytes();

    byte[] limitBytes = Ints.toByteArray(limit);

    return ByteBuffer.allocate(1 + orderByBytes.length + limitBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(orderByBytes)
                     .put(limitBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "DefaultLimitSpec{" +
           "orderBy='" + orderBy + '\'' +
           ", limit=" + limit +
           '}';
  }

  private Comparator<Row> makeComparator()
  {
    Ordering<Row> ordering = new Ordering<Row>()
    {
      @Override
      public int compare(Row left, Row right)
      {
        return Longs.compare(left.getTimestampFromEpoch(), right.getTimestampFromEpoch());
      }
    };

    for (final String dimension : orderBy) {
      ordering = ordering.compound(
          new Comparator<Row>()
          {
            @Override
            public int compare(Row left, Row right)
            {
              if (left instanceof MapBasedRow && right instanceof MapBasedRow) {
                // There are no multi-value dimensions at this point, they should have been flattened out
                String leftDimVal = left.getDimension(dimension).get(0);
                String rightDimVal = right.getDimension(dimension).get(0);
                return leftDimVal.compareTo(rightDimVal);
              } else {
                throw new ISE("Unknown type for rows[%s, %s]", left.getClass(), right.getClass());
              }
            }
          }
      );
    }
    final Ordering<Row> theOrdering = ordering;

    return new Comparator<Row>()
    {
      @Override
      public int compare(Row row, Row row2)
      {
        return theOrdering.compare(row, row2);
      }
    };
  }
}
