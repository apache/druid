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
import java.util.Set;
import java.util.TreeSet;

/**
 * Selects projections whose names match any of the configured glob patterns, minus any names listed in
 * {@code excludes}. Matching uses {@link FilenameUtils#wildcardMatch(String, String)}; supported glob metacharacters
 * are {@code *} (any sequence of characters) and {@code ?} (single character), all other characters literal. Excludes
 * are literal projection names and let an operator carve out specific names from a broad pattern, for example, a
 * long-retention rule {@code patterns=["user_*"]} combined with {@code excludes=["user_daily"]} keeps every
 * {@code user_*} projection except {@code user_daily}, which the operator intends to retain only via a
 * shorter-retention exact-match rule.
 */
public class WildcardProjectionPartialLoadMatcher extends ProjectionPartialLoadMatcher
{
  public static final String TYPE = "globProjection";

  private final List<String> patterns;
  private final List<String> excludes;
  private final Set<String> excludeSet;

  @JsonCreator
  public WildcardProjectionPartialLoadMatcher(
      @JsonProperty("patterns") List<String> patterns,
      @JsonProperty("excludes") @Nullable List<String> excludes
  )
  {
    if (patterns == null || patterns.isEmpty()) {
      throw InvalidInput.exception("patterns must not be null or empty for globProjection matcher");
    }
    this.patterns = List.copyOf(patterns);
    this.excludes = excludes == null ? List.of() : List.copyOf(excludes);
    this.excludeSet = this.excludes.isEmpty() ? Set.of() : Set.copyOf(this.excludes);
  }

  @JsonProperty
  public List<String> getPatterns()
  {
    return patterns;
  }

  @JsonProperty
  public List<String> getExcludes()
  {
    return excludes;
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
      if (excludeSet.contains(name)) {
        continue;
      }
      for (String pattern : patterns) {
        if (FilenameUtils.wildcardMatch(name, pattern)) {
          matched.add(name);
          break;
        }
      }
    }
    return new ArrayList<>(matched);
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
    return Objects.equals(patterns, that.patterns) && Objects.equals(excludes, that.excludes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(patterns, excludes);
  }

  @Override
  public String toString()
  {
    return "WildcardProjectionPartialLoadMatcher{patterns=" + patterns + ", excludes=" + excludes + "}";
  }
}
