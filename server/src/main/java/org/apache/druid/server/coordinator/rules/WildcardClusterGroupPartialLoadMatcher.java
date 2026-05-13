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
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Selects cluster groups whose clustering tuples match any of the configured per-column glob patterns, minus any
 * groups matched by an entry in {@code excludePatterns}. Each pattern is a {@code Map<String, String>} where keys are
 * clustering column names and values are glob patterns matched against the rendered tuple value at that column.
 * Columns omitted from a pattern are treated as wildcards. Glob syntax (including escape semantics) is shared with
 * {@link WildcardProjectionPartialLoadMatcher} via {@link Globs}.
 * <p>
 * Null clustering values are matched only by omitting the column entirely or using the literal {@code "*"} glob; both
 * have explicit "match-anything-including-null" semantics. Any other glob (including {@code "null"}) does not match a
 * null clustering value; for those cases use {@link ExactClusterGroupPartialLoadMatcher}.
 */
public class WildcardClusterGroupPartialLoadMatcher extends ClusterGroupPartialLoadMatcher
{
  public static final String TYPE = "globClusterGroup";

  private final List<Map<String, String>> patterns;
  private final List<Map<String, String>> excludePatterns;
  private final List<Map<String, CompiledGlob>> compiledPatterns;
  private final List<Map<String, CompiledGlob>> compiledExcludePatterns;

  @JsonCreator
  public WildcardClusterGroupPartialLoadMatcher(
      @JsonProperty("patterns") List<Map<String, String>> patterns,
      @JsonProperty("excludePatterns") @Nullable List<Map<String, String>> excludePatterns
  )
  {
    if (patterns == null || patterns.isEmpty()) {
      throw InvalidInput.exception("patterns must not be null or empty for globClusterGroup matcher");
    }
    this.patterns = copyAndValidatePatterns(patterns, "patterns");
    this.excludePatterns = excludePatterns == null
                           ? List.of()
                           : copyAndValidatePatterns(excludePatterns, "excludePatterns");
    this.compiledPatterns = compileAll(this.patterns);
    this.compiledExcludePatterns = compileAll(this.excludePatterns);
  }

  @JsonProperty
  public List<Map<String, String>> getPatterns()
  {
    return patterns;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<Map<String, String>> getExcludePatterns()
  {
    return excludePatterns;
  }

  @Override
  protected List<Integer> resolveClusterGroupIndices(DataSegment segment)
  {
    final ClusterGroupTuples clusterGroups = segment.getClusterGroups();
    if (clusterGroups == null) {
      return Collections.emptyList();
    }
    final RowSignature clusteringColumns = clusterGroups.getClusteringColumns();
    final List<List<Object>> tuples = clusterGroups.getTuples();
    final TreeSet<Integer> matched = new TreeSet<>();
    for (int i = 0; i < tuples.size(); i++) {
      final List<Object> tuple = tuples.get(i);
      if (!matchesAnyPattern(tuple, clusteringColumns, compiledPatterns)) {
        continue;
      }
      if (matchesAnyPattern(tuple, clusteringColumns, compiledExcludePatterns)) {
        continue;
      }
      matched.add(i);
    }
    return new ArrayList<>(matched);
  }

  private static boolean matchesAnyPattern(
      List<Object> tuple,
      RowSignature clusteringColumns,
      List<Map<String, CompiledGlob>> patterns
  )
  {
    for (Map<String, CompiledGlob> pattern : patterns) {
      if (matchesPattern(tuple, clusteringColumns, pattern)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesPattern(
      List<Object> tuple,
      RowSignature clusteringColumns,
      Map<String, CompiledGlob> pattern
  )
  {
    for (Map.Entry<String, CompiledGlob> entry : pattern.entrySet()) {
      final int idx = clusteringColumns.indexOf(entry.getKey());
      if (idx < 0) {
        // Pattern references a column the segment doesn't have. Defensive: a typo shouldn't silently broaden the
        // match, so treat the whole pattern as non-matching for this segment.
        return false;
      }
      final CompiledGlob glob = entry.getValue();
      if (glob.matchAny) {
        // Literal "*" — matches every value, including null.
        continue;
      }
      final Object value = tuple.get(idx);
      if (value == null) {
        // Any non-"*" glob cannot match a null clustering value. Use ExactClusterGroupPartialLoadMatcher for that.
        return false;
      }
      if (!glob.pattern.matcher(value.toString()).matches()) {
        return false;
      }
    }
    return true;
  }

  private static List<Map<String, String>> copyAndValidatePatterns(List<Map<String, String>> input, String fieldName)
  {
    final List<Map<String, String>> out = new ArrayList<>(input.size());
    for (int i = 0; i < input.size(); i++) {
      final Map<String, String> pattern = input.get(i);
      if (pattern == null || pattern.isEmpty()) {
        throw InvalidInput.exception("%s[%s] must not be null or empty", fieldName, i);
      }
      out.add(Map.copyOf(pattern));
    }
    return List.copyOf(out);
  }

  private static List<Map<String, CompiledGlob>> compileAll(List<Map<String, String>> patterns)
  {
    if (patterns.isEmpty()) {
      return List.of();
    }
    final List<Map<String, CompiledGlob>> out = new ArrayList<>(patterns.size());
    for (Map<String, String> pattern : patterns) {
      final Map<String, CompiledGlob> compiled = CollectionUtils.newLinkedHashMapWithExpectedSize(pattern.size());
      for (Map.Entry<String, String> entry : pattern.entrySet()) {
        compiled.put(entry.getKey(), CompiledGlob.of(entry.getValue()));
      }
      out.add(Collections.unmodifiableMap(compiled));
    }
    return List.copyOf(out);
  }

  /**
   * Compiled per-column glob. The literal {@code "*"} short-circuits to a "match any value, including null" flag;
   * any other glob compiles to a regex matched against the rendered tuple value (and never matches null).
   */
  private static final class CompiledGlob
  {
    static final CompiledGlob MATCH_ANY = new CompiledGlob(true, null);

    final boolean matchAny;
    @Nullable
    final Pattern pattern;

    private CompiledGlob(boolean matchAny, @Nullable Pattern pattern)
    {
      this.matchAny = matchAny;
      this.pattern = pattern;
    }

    static CompiledGlob of(String glob)
    {
      if ("*".equals(glob)) {
        return MATCH_ANY;
      }
      return new CompiledGlob(false, Pattern.compile(Globs.globToRegex(glob)));
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
    WildcardClusterGroupPartialLoadMatcher that = (WildcardClusterGroupPartialLoadMatcher) o;
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
    return "WildcardClusterGroupPartialLoadMatcher{patterns=" + patterns
           + ", excludePatterns=" + excludePatterns + "}";
  }
}
