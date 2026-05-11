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

package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Selects projections whose names match any of the configured glob patterns, minus any names matching an entry in
 * {@code excludePatterns}. Supported glob metacharacters:
 * <ul>
 *   <li>{@code *} — any sequence of characters (including empty)</li>
 *   <li>{@code ?} — any single character</li>
 *   <li>{@code \} — escapes the following character so it is treated literally; use {@code \*}, {@code \?}, or
 *       {@code \\} to match a literal {@code *}, {@code ?}, or {@code \}. A trailing unescaped {@code \} is
 *       rejected at construction.</li>
 * </ul>
 * All other characters are literal; regex metacharacters are escaped automatically. A literal projection name is
 * a valid (zero-wildcard) glob, so the same {@code excludePatterns} field covers both "exclude this specific name"
 * and "exclude anything matching this pattern."
 * <p>
 * For example, a long-retention rule {@code patterns=["user_*"], excludePatterns=["user_daily"]} keeps every
 * {@code user_*} projection except {@code user_daily} (which is expected to live on a shorter-retention rule). A
 * broad rule {@code patterns=["*"], excludePatterns=["user_*"]} loads every projection except those handled by a
 * more specific {@code user_*} rule elsewhere in the cascade.
 */
public class WildcardProjectionPartialLoadMatcher extends ProjectionPartialLoadMatcher
{
  public static final String TYPE = "globProjection";

  private final List<String> patterns;
  private final List<String> excludePatterns;
  private final List<Pattern> compiledPatterns;
  private final List<Pattern> compiledExcludePatterns;

  @JsonCreator
  public WildcardProjectionPartialLoadMatcher(
      @JsonProperty("patterns") List<String> patterns,
      @JsonProperty("excludePatterns") @Nullable List<String> excludePatterns
  )
  {
    if (patterns == null || patterns.isEmpty()) {
      throw InvalidInput.exception("patterns must not be null or empty for globProjection matcher");
    }
    this.patterns = List.copyOf(patterns);
    this.excludePatterns = excludePatterns == null ? List.of() : List.copyOf(excludePatterns);
    this.compiledPatterns = compileAll(this.patterns);
    this.compiledExcludePatterns = compileAll(this.excludePatterns);
  }

  @JsonProperty
  public List<String> getPatterns()
  {
    return patterns;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getExcludePatterns()
  {
    return excludePatterns;
  }

  @Override
  protected List<String> resolveProjectionNames(DataSegment segment)
  {
    final List<String> segmentProjections = segment.getProjections();
    if (segmentProjections == null || segmentProjections.isEmpty()) {
      return Collections.emptyList();
    }
    final TreeSet<String> matched = new TreeSet<>();
    for (String name : segmentProjections) {
      if (matchesAny(name, compiledExcludePatterns)) {
        continue;
      }
      if (matchesAny(name, compiledPatterns)) {
        matched.add(name);
      }
    }
    return new ArrayList<>(matched);
  }

  private static List<Pattern> compileAll(List<String> globs)
  {
    if (globs.isEmpty()) {
      return List.of();
    }
    final List<Pattern> compiled = new ArrayList<>(globs.size());
    for (String glob : globs) {
      compiled.add(Pattern.compile(globToRegex(glob)));
    }
    return List.copyOf(compiled);
  }

  private static boolean matchesAny(String name, List<Pattern> patterns)
  {
    for (Pattern pattern : patterns) {
      if (pattern.matcher(name).matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Translates a glob pattern with {@code *}, {@code ?}, and {@code \} escape semantics into an equivalent regex
   * pattern that matches the entire input string. Regex metacharacters in literal positions are escaped.
   *
   * @throws org.apache.druid.error.DruidException if {@code glob} ends with an unescaped backslash
   */
  static String globToRegex(String glob)
  {
    final StringBuilder sb = new StringBuilder(glob.length() + 4);
    boolean escaping = false;
    for (int i = 0; i < glob.length(); i++) {
      final char c = glob.charAt(i);
      if (escaping) {
        appendLiteral(sb, c);
        escaping = false;
        continue;
      }
      switch (c) {
        case '\\':
          escaping = true;
          break;
        case '*':
          sb.append(".*");
          break;
        case '?':
          sb.append('.');
          break;
        default:
          appendLiteral(sb, c);
      }
    }
    if (escaping) {
      throw InvalidInput.exception("Glob pattern [%s] ends with an unescaped backslash", glob);
    }
    return sb.toString();
  }

  private static void appendLiteral(StringBuilder sb, char c)
  {
    switch (c) {
      case '.':
      case '(':
      case ')':
      case '[':
      case ']':
      case '{':
      case '}':
      case '+':
      case '|':
      case '^':
      case '$':
      case '\\':
      case '*':
      case '?':
        sb.append('\\').append(c);
        break;
      default:
        sb.append(c);
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
    WildcardProjectionPartialLoadMatcher that = (WildcardProjectionPartialLoadMatcher) o;
    return Objects.equals(patterns, that.patterns) && Objects.equals(excludePatterns, that.excludePatterns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(patterns, excludePatterns);
  }

  @Override
  public String toString()
  {
    return "WildcardProjectionPartialLoadMatcher{patterns=" + patterns + ", excludePatterns=" + excludePatterns + "}";
  }
}
