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
public class MatchingDimExtractionFn extends DimExtractionFn
{
  private final String expr;
  private final Pattern pattern;

  @JsonCreator
  public MatchingDimExtractionFn(
      @JsonProperty("expr") String expr
  )
  {
    Preconditions.checkNotNull(expr, "expr must not be null");

    this.expr = expr;
    this.pattern = Pattern.compile(expr);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] exprBytes = StringUtils.toUtf8(expr);
    return ByteBuffer.allocate(1 + exprBytes.length)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_MATCHING_DIM)
                     .put(exprBytes)
                     .array();
  }

  @Override
  public String apply(String dimValue)
  {
    if (Strings.isNullOrEmpty(dimValue)) {
      // We'd return null whether or not the pattern matched
      return null;
    }

    Matcher matcher = pattern.matcher(dimValue);
    return matcher.find() ? dimValue : null;
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
  public ExtractionType getExtractionType()
  {
    return ExtractionType.MANY_TO_ONE;
  }

  @Override
  public String toString()
  {
    return String.format("regex_matches(%s)", expr);
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

    MatchingDimExtractionFn that = (MatchingDimExtractionFn) o;

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
