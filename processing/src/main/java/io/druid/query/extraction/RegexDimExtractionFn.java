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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class RegexDimExtractionFn implements DimExtractionFn
{
  private static final byte CACHE_TYPE_ID = 0x1;

  private final String expr;
  private final Pattern pattern;

  @JsonCreator
  public RegexDimExtractionFn(
      @JsonProperty("expr") String expr
  )
  {
    this.expr = expr;
    this.pattern = Pattern.compile(expr);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] exprBytes = expr.getBytes(Charsets.UTF_8);
    return ByteBuffer.allocate(1 + exprBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(exprBytes)
                     .array();
  }

  @Override
  public String apply(String dimValue)
  {
    Matcher matcher = pattern.matcher(dimValue);
    return matcher.find() ? matcher.group(1) : dimValue;
  }

  @JsonProperty("expr")
  public String getExpr()
  {
    return expr;
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return String.format("regex(%s)", expr);
  }
}
