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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Chars;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.filter.LikeFilter;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

public class LikeDimFilter extends AbstractOptimizableDimFilter implements DimFilter
{
  // Regex matching characters that are definitely okay to include unescaped in a regex.
  // Leads to excessively paranoid escaping, although shouldn't affect runtime beyond compiling the regex.
  private static final Pattern DEFINITELY_FINE = Pattern.compile("[\\w\\d\\s-]");
  private static final String WILDCARD = ".*";

  private final String dimension;
  private final String pattern;
  @Nullable
  private final Character escapeChar;
  @Nullable
  private final ExtractionFn extractionFn;
  @Nullable
  private final FilterTuning filterTuning;
  private final LikeMatcher likeMatcher;

  @JsonCreator
  public LikeDimFilter(
      @JsonProperty("dimension") final String dimension,
      @JsonProperty("pattern") final String pattern,
      @JsonProperty("escape") @Nullable final String escape,
      @JsonProperty("extractionFn") @Nullable final ExtractionFn extractionFn,
      @JsonProperty("filterTuning") @Nullable final FilterTuning filterTuning
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");
    this.pattern = Preconditions.checkNotNull(pattern, "pattern");
    this.extractionFn = extractionFn;
    this.filterTuning = filterTuning;

    if (escape != null && escape.length() != 1) {
      throw new IllegalArgumentException("Escape must be null or a single character");
    } else {
      this.escapeChar = (escape == null || escape.isEmpty()) ? null : escape.charAt(0);
    }

    this.likeMatcher = LikeMatcher.from(pattern, this.escapeChar);
  }

  @VisibleForTesting
  public LikeDimFilter(
      final String dimension,
      final String pattern,
      @Nullable final String escape,
      @Nullable final ExtractionFn extractionFn
  )
  {
    this(dimension, pattern, escape, extractionFn, null);
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getPattern()
  {
    return pattern;
  }

  @Nullable
  @JsonProperty
  public String getEscape()
  {
    return escapeChar != null ? escapeChar.toString() : null;
  }

  @Nullable
  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    final byte[] patternBytes = StringUtils.toUtf8(pattern);
    final byte[] escapeBytes = escapeChar == null ? new byte[0] : Chars.toByteArray(escapeChar);
    final byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();
    final int sz = 4 + dimensionBytes.length + patternBytes.length + escapeBytes.length + extractionFnBytes.length;
    return ByteBuffer.allocate(sz)
                     .put(DimFilterUtils.LIKE_CACHE_ID)
                     .put(dimensionBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(patternBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(escapeBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(extractionFnBytes)
                     .array();
  }

  @Override
  public Filter toFilter()
  {
    return new LikeFilter(dimension, extractionFn, likeMatcher, filterTuning);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
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
    LikeDimFilter that = (LikeDimFilter) o;
    return dimension.equals(that.dimension) &&
           pattern.equals(that.pattern) &&
           Objects.equals(escapeChar, that.escapeChar) &&
           Objects.equals(extractionFn, that.extractionFn) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, pattern, escapeChar, extractionFn, filterTuning);
  }

  @Override
  public String toString()
  {
    final DimFilterToStringBuilder builder = new DimFilterToStringBuilder();
    builder.appendDimension(dimension, extractionFn).append(" LIKE '").append(pattern).append("'");
    if (escapeChar != null) {
      builder.append(" ESCAPE '").append(escapeChar).append("'");
    }
    return builder.appendFilterTuning(filterTuning).build();
  }

  public static class LikeMatcher
  {
    public enum SuffixMatch
    {
      MATCH_ANY,
      MATCH_EMPTY,
      MATCH_PATTERN
    }

    // Strings match if:
    //  (a) suffixMatch is MATCH_ANY and they start with "prefix"
    //  (b) suffixMatch is MATCH_EMPTY and they start with "prefix" and contain nothing after prefix
    //  (c) suffixMatch is MATCH_PATTERN and the string matches "pattern"
    private final SuffixMatch suffixMatch;

    // Prefix that matching strings are known to start with. May be empty.
    private final String prefix;

    // Regex pattern that describes matching strings.
    private final Pattern pattern;

    private LikeMatcher(
        final SuffixMatch suffixMatch,
        final String prefix,
        final Pattern pattern
    )
    {
      this.suffixMatch = Preconditions.checkNotNull(suffixMatch, "suffixMatch");
      this.prefix = NullHandling.nullToEmptyIfNeeded(prefix);
      this.pattern = Preconditions.checkNotNull(pattern, "pattern");
    }

    public static LikeMatcher from(
        final String likePattern,
        @Nullable final Character escapeChar
    )
    {
      final StringBuilder prefix = new StringBuilder();
      final StringBuilder regex = new StringBuilder();
      boolean escaping = false;
      boolean inPrefix = true;
      SuffixMatch suffixMatch = SuffixMatch.MATCH_EMPTY;
      for (int i = 0; i < likePattern.length(); i++) {
        final char c = likePattern.charAt(i);
        if (escapeChar != null && c == escapeChar && !escaping) {
          escaping = true;
        } else if (c == '%' && !escaping) {
          inPrefix = false;
          if (suffixMatch == SuffixMatch.MATCH_EMPTY) {
            suffixMatch = SuffixMatch.MATCH_ANY;
          }
          regex.append(WILDCARD);
        } else if (c == '_' && !escaping) {
          inPrefix = false;
          suffixMatch = SuffixMatch.MATCH_PATTERN;
          regex.append(".");
        } else {
          if (inPrefix) {
            prefix.append(c);
          } else {
            suffixMatch = SuffixMatch.MATCH_PATTERN;
          }
          addPatternCharacter(regex, c);
          escaping = false;
        }
      }

      return new LikeMatcher(suffixMatch, prefix.toString(), Pattern.compile(regex.toString(), Pattern.DOTALL));
    }

    private static void addPatternCharacter(final StringBuilder patternBuilder, final char c)
    {
      if (DEFINITELY_FINE.matcher(String.valueOf(c)).matches()) {
        patternBuilder.append(c);
      } else {
        patternBuilder.append("\\u").append(BaseEncoding.base16().encode(Chars.toByteArray(c)));
      }
    }

    public DruidPredicateMatch matches(@Nullable final String s)
    {
      return matches(s, pattern);
    }

    private static DruidPredicateMatch matches(@Nullable final String s, Pattern pattern)
    {
      String val = NullHandling.nullToEmptyIfNeeded(s);
      if (val == null) {
        return DruidPredicateMatch.UNKNOWN;
      }
      return DruidPredicateMatch.of(pattern.matcher(val).matches());
    }

    /**
     * Checks if the suffix of "value" matches the suffix of this matcher. The first prefix.length() characters
     * of "value" are ignored. This method is useful if you've already independently verified the prefix.
     */
    public DruidPredicateMatch matchesSuffixOnly(@Nullable String value)
    {
      if (suffixMatch == SuffixMatch.MATCH_ANY) {
        return DruidPredicateMatch.TRUE;
      } else if (suffixMatch == SuffixMatch.MATCH_EMPTY) {
        return value == null ? matches(null) : DruidPredicateMatch.of(value.length() == prefix.length());
      } else {
        // suffixMatch is MATCH_PATTERN
        return matches(value);
      }
    }

    public DruidPredicateFactory predicateFactory(final ExtractionFn extractionFn)
    {
      return new PatternDruidPredicateFactory(extractionFn, pattern);
    }

    public String getPrefix()
    {
      return prefix;
    }

    public SuffixMatch getSuffixMatch()
    {
      return suffixMatch;
    }

    @VisibleForTesting
    static class PatternDruidPredicateFactory implements DruidPredicateFactory
    {
      private final ExtractionFn extractionFn;
      private final Pattern pattern;

      PatternDruidPredicateFactory(ExtractionFn extractionFn, Pattern pattern)
      {
        this.extractionFn = extractionFn;
        this.pattern = pattern;
      }

      @Override
      public DruidObjectPredicate<String> makeStringPredicate()
      {
        if (extractionFn != null) {
          return input -> matches(extractionFn.apply(input), pattern);
        } else {
          return input -> matches(input, pattern);
        }
      }

      @Override
      public DruidLongPredicate makeLongPredicate()
      {
        if (extractionFn != null) {
          return input -> matches(extractionFn.apply(input), pattern);
        } else {
          return input -> matches(String.valueOf(input), pattern);
        }
      }

      @Override
      public DruidFloatPredicate makeFloatPredicate()
      {
        if (extractionFn != null) {
          return input -> matches(extractionFn.apply(input), pattern);
        } else {
          return input -> matches(String.valueOf(input), pattern);
        }
      }

      @Override
      public DruidDoublePredicate makeDoublePredicate()
      {
        if (extractionFn != null) {
          return input -> matches(extractionFn.apply(input), pattern);
        } else {
          return input -> matches(String.valueOf(input), pattern);
        }
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
        PatternDruidPredicateFactory that = (PatternDruidPredicateFactory) o;
        return Objects.equals(extractionFn, that.extractionFn) &&
               Objects.equals(pattern.toString(), that.pattern.toString());
      }

      @Override
      public int hashCode()
      {
        return Objects.hash(extractionFn, pattern.toString());
      }
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
      LikeMatcher that = (LikeMatcher) o;
      return getSuffixMatch() == that.getSuffixMatch() &&
             Objects.equals(getPrefix(), that.getPrefix()) &&
             Objects.equals(pattern.toString(), that.pattern.toString());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(getSuffixMatch(), getPrefix(), pattern.toString());
    }
  }
}
