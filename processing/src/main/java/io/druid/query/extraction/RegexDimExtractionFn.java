/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class RegexDimExtractionFn extends DimExtractionFn
{
  private static final byte CACHE_KEY_SEPARATOR = (byte) 0xFF;

  private final String expr;
  private final Pattern pattern;
  private final boolean replaceMissingValue;
  private final String replaceMissingValueWith;

  @JsonCreator
  public RegexDimExtractionFn(
      @JsonProperty("expr") String expr,
      @JsonProperty("replaceMissingValue") Boolean replaceMissingValue,
      @JsonProperty("replaceMissingValueWith") String replaceMissingValueWith
  )
  {
    Preconditions.checkNotNull(expr, "expr must not be null");

    this.expr = expr;
    this.pattern = Pattern.compile(expr);
    this.replaceMissingValue = replaceMissingValue == null ? false : replaceMissingValue;
    this.replaceMissingValueWith = replaceMissingValueWith;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] exprBytes = StringUtils.toUtf8(expr);
    byte[] replaceBytes = replaceMissingValue ? new byte[]{1} : new byte[]{0};
    byte[] replaceStrBytes;
    if (replaceMissingValueWith == null) {
      replaceStrBytes = new byte[]{};
    } else {
      replaceStrBytes = StringUtils.toUtf8(replaceMissingValueWith);
    }

    int totalLen = 1
                   + exprBytes.length
                   + replaceBytes.length
                   + replaceStrBytes.length; // fields
    totalLen += 2; // separators

    return ByteBuffer.allocate(totalLen)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_REGEX)
                     .put(exprBytes)
                     .put(CACHE_KEY_SEPARATOR)
                     .put(replaceStrBytes)
                     .put(CACHE_KEY_SEPARATOR)
                     .put(replaceBytes)
                     .array();
  }

  @Override
  public String apply(String dimValue)
  {
    final String retVal;
    final Matcher matcher = pattern.matcher(Strings.nullToEmpty(dimValue));
    if (matcher.find()) {
      retVal = matcher.group(1);
    } else {
      retVal = replaceMissingValue ? replaceMissingValueWith : dimValue;
    }
    return Strings.emptyToNull(retVal);
  }

  @JsonProperty("expr")
  public String getExpr()
  {
    return expr;
  }

  @JsonProperty("replaceMissingValue")
  public boolean isReplaceMissingValue()
  {
    return replaceMissingValue;
  }

  @JsonProperty("replaceMissingValueWith")
  public String getReplaceMissingValueWith()
  {
    return replaceMissingValueWith;
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return ExtractionType.MANY_TO_ONE;
  }

  @Override
  public String toString()
  {
    return String.format("regex(%s)", expr);
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

    RegexDimExtractionFn that = (RegexDimExtractionFn) o;

    if (!expr.equals(that.expr)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return expr.hashCode();
  }
}
