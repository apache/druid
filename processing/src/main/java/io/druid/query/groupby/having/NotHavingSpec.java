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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.Row;

import java.nio.ByteBuffer;

/**
 * The logical "not" operator for the "having" clause.
 */
public class NotHavingSpec implements HavingSpec
{
  private static final byte CACHE_KEY = 0x6;

  private HavingSpec havingSpec;

  @JsonCreator
  public NotHavingSpec(@JsonProperty("havingSpec") HavingSpec havingSpec)
  {
    this.havingSpec = havingSpec;
  }

  @JsonProperty("havingSpec")
  public HavingSpec getHavingSpec()
  {
    return havingSpec;
  }

  @Override
  public boolean eval(Row row)
  {
    return !havingSpec.eval(row);
  }

  @Override
  public byte[] getCacheKey()
  {
    return ByteBuffer.allocate(1 + havingSpec.getCacheKey().length)
                     .put(CACHE_KEY)
                     .put(havingSpec.getCacheKey())
                     .array();
  }

  @Override
  public String toString()
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("NotHavingSpec");
    sb.append("{havingSpec=").append(havingSpec);
    sb.append('}');
    return sb.toString();
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

    NotHavingSpec that = (NotHavingSpec) o;

    if (havingSpec != null ? !havingSpec.equals(that.havingSpec) : that.havingSpec != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return havingSpec != null ? havingSpec.hashCode() : 0;
  }
}
