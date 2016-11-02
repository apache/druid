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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.RangeSet;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Chars;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.filter.LikeFilter;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

public class LikeDimFilter implements DimFilter
{
  // Regex matching characters that are definitely okay to include unescaped in a regex.
  // Leads to excessively paranoid escaping, although shouldn't affect runtime beyond compiling the regex.
  private static final Pattern DEFINITELY_FINE = Pattern.compile("[\\w\\d\\s-]");
  private static final String WILDCARD = ".*";

  private final String dimension;
  private final String pattern;
  private final Character escapeChar;
  private final ExtractionFn extractionFn;
  private final LikeMatcher likeMatcher;

  @JsonCreator
  public LikeDimFilter(
      @JsonProperty("dimension") final String dimension,
      @JsonProperty("pattern") final String pattern,
      @JsonProperty("escape") final String escape,
      @JsonProperty("extractionFn") final ExtractionFn extractionFn
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");
    this.pattern = Preconditions.checkNotNull(pattern, "pattern");
    this.extractionFn = extractionFn;

    if (escape != null && escape.length() != 1) {
      throw new IllegalArgumentException("Escape must be null or a single character");
    } else {
      this.escapeChar = (escape == null || escape.isEmpty()) ? null : escape.charAt(0);
    }

    this.likeMatcher = LikeMatcher.from(pattern, this.escapeChar, extractionFn == null);
  }

  public static class LikeMatcher
  {
    public enum SuffixStyle
    {
      MATCH_ANY,
      MATCH_EMPTY,
      MATCH_PATTERN
    }

    // Strings match if they start with "prefix" AND:
    //  (a) suffixStyle is MATCH_ANY
    //  (b) suffixStyle is MATCH_EMPTY and there is nothing after prefix
    //  (c) suffixStyle is MATCH_PATTERN and the part after prefix matches suffixPattern
    private final String prefix;
    private final SuffixStyle suffixStyle;
    private final Pattern suffixPattern;

    // Strings match if they match "pattern".
    private final Pattern pattern;

    private LikeMatcher(
        final String prefix,
        final SuffixStyle suffixStyle,
        final Pattern suffixPattern,
        final Pattern pattern
    )
    {
      this.prefix = Strings.nullToEmpty(prefix);
      this.suffixStyle = Preconditions.checkNotNull(suffixStyle, "suffixStyle");
      this.suffixPattern = suffixPattern;
      this.pattern = Preconditions.checkNotNull(pattern, "pattern");

      if (suffixPattern == null && suffixStyle == SuffixStyle.MATCH_PATTERN) {
        throw new IAE("Need suffixPattern for pattern matching!");
      } else if (suffixPattern != null && suffixStyle != SuffixStyle.MATCH_PATTERN) {
        throw new IAE("Can't have suffixPattern without pattern matching!");
      }
    }

    public static LikeMatcher from(
        final String likePattern,
        @Nullable final Character escapeChar,
        final boolean extractPrefix
    )
    {
      final StringBuilder prefix = new StringBuilder();
      final StringBuilder suffixRegex = new StringBuilder();
      final StringBuilder regex = new StringBuilder();
      boolean escaping = false;
      boolean inPrefix = true;
      for (int i = 0; i < likePattern.length(); i++) {
        final char c = likePattern.charAt(i);
        if (escapeChar != null && c == escapeChar) {
          escaping = true;
        } else if (c == '%' && !escaping) {
          inPrefix = false;
          suffixRegex.append(WILDCARD);
          regex.append(WILDCARD);
        } else if (c == '_' && !escaping) {
          inPrefix = false;
          suffixRegex.append(".");
          regex.append(".");
        } else {
          if (inPrefix && extractPrefix) {
            prefix.append(c);
          } else {
            addPatternCharacter(suffixRegex, c);
          }
          addPatternCharacter(regex, c);
          escaping = false;
        }
      }

      final String suffixRegexString = suffixRegex.toString();

      if (suffixRegexString.isEmpty()) {
        return new LikeMatcher(prefix.toString(), SuffixStyle.MATCH_EMPTY, null, Pattern.compile(regex.toString()));
      } else if (suffixRegexString.equals(WILDCARD)) {
        return new LikeMatcher(prefix.toString(), SuffixStyle.MATCH_ANY, null, Pattern.compile(regex.toString()));
      } else {
        return new LikeMatcher(
            prefix.toString(),
            SuffixStyle.MATCH_PATTERN,
            Pattern.compile(suffixRegexString),
            Pattern.compile(regex.toString())
        );
      }
    }

    private static void addPatternCharacter(final StringBuilder patternBuilder, final char c)
    {
      if (DEFINITELY_FINE.matcher(String.valueOf(c)).matches()) {
        patternBuilder.append(c);
      } else {
        patternBuilder.append("\\u").append(BaseEncoding.base16().encode(Chars.toByteArray(c)));
      }
    }

    public boolean matches(@Nullable final String s)
    {
      return pattern.matcher(Strings.nullToEmpty(s)).matches();
    }

    /**
     * Checks if the suffix of "s" matches the suffix of this matcher. The first prefix.length characters
     * of s are ignored. This method is useful if you've already independently verified the prefix.
     */
    public boolean matchesSuffixOnly(@Nullable final String s)
    {
      if (suffixStyle == SuffixStyle.MATCH_ANY) {
        return true;
      } else if (suffixStyle == SuffixStyle.MATCH_EMPTY) {
        return s == null || s.length() == prefix.length();
      } else {
        // suffixStyle is MATCH_PATTERN
        return suffixPattern.matcher(Strings.nullToEmpty(s).substring(prefix.length())).matches();
      }
    }

    public DruidPredicateFactory predicateFactory(final ExtractionFn extractionFn)
    {
      return new DruidPredicateFactory()
      {
        @Override
        public Predicate<String> makeStringPredicate()
        {
          if (extractionFn != null) {
            return new Predicate<String>()
            {
              @Override
              public boolean apply(String input)
              {
                return matches(extractionFn.apply(input));
              }
            };
          } else {
            return new Predicate<String>()
            {
              @Override
              public boolean apply(String input)
              {
                return matches(input);
              }
            };
          }
        }

        @Override
        public DruidLongPredicate makeLongPredicate()
        {
          if (extractionFn != null) {
            return new DruidLongPredicate()
            {
              @Override
              public boolean applyLong(long input)
              {
                return matches(extractionFn.apply(input));
              }
            };
          } else {
            return new DruidLongPredicate()
            {
              @Override
              public boolean applyLong(long input)
              {
                return matches(String.valueOf(input));
              }
            };
          }
        }
      };
    }

    public String getPrefix()
    {
      return prefix;
    }

    public SuffixStyle getSuffixStyle()
    {
      return suffixStyle;
    }
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

  @JsonProperty
  public String getEscape()
  {
    return escapeChar != null ? escapeChar.toString() : null;
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
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
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new LikeFilter(dimension, extractionFn, likeMatcher);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
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

    if (dimension != null ? !dimension.equals(that.dimension) : that.dimension != null) {
      return false;
    }
    if (pattern != null ? !pattern.equals(that.pattern) : that.pattern != null) {
      return false;
    }
    if (escapeChar != null ? !escapeChar.equals(that.escapeChar) : that.escapeChar != null) {
      return false;
    }
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;

  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (pattern != null ? pattern.hashCode() : 0);
    result = 31 * result + (escapeChar != null ? escapeChar.hashCode() : 0);
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    final StringBuilder builder = new StringBuilder();

    if (extractionFn != null) {
      builder.append(extractionFn).append("(");
    }

    builder.append(dimension);

    if (extractionFn != null) {
      builder.append(")");
    }

    builder.append(" LIKE '").append(pattern).append("'");

    if (escapeChar != null) {
      builder.append(" ESCAPE '").append(escapeChar).append("'");
    }

    return builder.toString();
  }
}
