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
import com.metamx.common.StringUtils;

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
  private final boolean replaceMissingValues;
  private final String replaceMissingValuesWith;

  @JsonCreator
  public RegexDimExtractionFn(
      @JsonProperty("expr") String expr,
      @JsonProperty("replaceMissingValues") Boolean replaceMissingValues,
      @JsonProperty("replaceMissingValuesWith") String replaceMissingValuesWith
  )
  {
    Preconditions.checkNotNull(expr, "expr must not be null");

    this.expr = expr;
    this.pattern = Pattern.compile(expr);
    this.replaceMissingValues = replaceMissingValues == null ? false : replaceMissingValues;
    this.replaceMissingValuesWith = replaceMissingValuesWith;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] exprBytes = StringUtils.toUtf8(expr);
    byte[] replaceBytes = replaceMissingValues ? new byte[]{1} : new byte[]{0};
    byte[] replaceStrBytes;
    if (replaceMissingValuesWith == null) {
      replaceStrBytes = new byte[]{};
    } else {
      replaceStrBytes = StringUtils.toUtf8(replaceMissingValuesWith);
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
    if (dimValue == null) {
      return null;
    }
    String retVal;
    Matcher matcher = pattern.matcher(dimValue);
    if (matcher.find()) {
      retVal = matcher.group(1);
    } else {
      retVal = replaceMissingValues ? replaceMissingValuesWith : dimValue;
    }
    return Strings.emptyToNull(retVal);
  }

  @JsonProperty("expr")
  public String getExpr()
  {
    return expr;
  }

  @JsonProperty("replaceMissingValues")
  public boolean isReplaceMissingValues()
  {
    return replaceMissingValues;
  }

  @JsonProperty("replaceMissingValuesWith")
  public String getReplaceMissingValuesWith()
  {
    return replaceMissingValuesWith;
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
