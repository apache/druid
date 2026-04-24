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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.io.FilenameUtils;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Selects projections whose names match any of the configured glob patterns, minus any names matching an entry in
 * {@code excludePatterns}. Matching uses {@link FilenameUtils#wildcardMatch(String, String)}; supported glob
 * metacharacters are {@code *} (any sequence of characters) and {@code ?} (single character), all other characters
 * literal. A literal projection name is also a valid glob (with no wildcards it matches exactly itself), so the same
 * field covers both "exclude this specific name" and "exclude anything matching this pattern."
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
  }

  @JsonProperty
  public List<String> getPatterns()
  {
    return patterns;
  }

  @JsonProperty
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
      if (matchesAny(name, excludePatterns)) {
        continue;
      }
      if (matchesAny(name, patterns)) {
        matched.add(name);
      }
    }
    return new ArrayList<>(matched);
  }

  private static boolean matchesAny(String name, List<String> patterns)
  {
    for (String pattern : patterns) {
      if (FilenameUtils.wildcardMatch(name, pattern)) {
        return true;
      }
    }
    return false;
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
