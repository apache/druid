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

package org.apache.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexDimReplacementFn extends DimExtractionFn
{
  private static final byte CACHE_KEY_SEPARATOR = (byte) 0xFF;

  @Nonnull private final String expr;
  @Nonnull private final Pattern pattern;
  @Nonnull private final String replacement;

  @JsonCreator
  public RegexDimReplacementFn(
      @Nullable @JsonProperty("expr") String expr,
      @JsonProperty("replacement") String replacement
  )
  {
    Preconditions.checkNotNull(expr, "expr must not be null");

    this.expr = expr;
    this.pattern = Pattern.compile(expr);
    this.replacement = StringUtils.nullToEmptyNonDruidDataString(replacement);
  }

  @Nullable
  @Override
  public String apply(@Nullable String value)
  {
    final String retVal;
    final String s = NullHandling.nullToEmptyIfNeeded(value);

    if (s == null) {
      // True nulls do not match anything. Note: this branch only executes in SQL-compatible null handling mode.
      retVal = null;
    } else {
      final Matcher matcher = pattern.matcher(s);
      StringBuffer sb = new StringBuffer();
      if (matcher.find()) {
        matcher.appendReplacement(sb, replacement);
        while (matcher.find()) {
          matcher.appendReplacement(sb, replacement);
        }
        matcher.appendTail(sb);
        return sb.toString();
      }
      retVal = value;
    }
    return NullHandling.emptyToNullIfNeeded(retVal);
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
  public byte[] getCacheKey()
  {
    byte[] exprBytes = StringUtils.toUtf8(expr);
    byte[] replacementBytes = StringUtils.toUtf8(replacement);
    int totalLen = 1 + exprBytes.length + 1 + replacementBytes.length;

    return ByteBuffer.allocate(totalLen)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_REGEX_REPLACE)
                     .put(exprBytes)
                     .put(CACHE_KEY_SEPARATOR)
                     .put(replacementBytes)
                     .array();
  }

  @Nonnull
  @JsonProperty("expr")
  public String getExpr()
  {
    return expr;
  }

  @Nonnull
  @JsonProperty("replacement")
  public String getReplacement()
  {
    return replacement;
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
    RegexDimReplacementFn that = (RegexDimReplacementFn) o;
    return expr.equals(that.expr) &&
           replacement.equals(that.replacement);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(expr, replacement);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("regex_replace(/%s/, %s)", expr, replacement);
  }
}
