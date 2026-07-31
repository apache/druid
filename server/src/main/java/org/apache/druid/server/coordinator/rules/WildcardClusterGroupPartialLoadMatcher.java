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
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Selects cluster groups whose clustering tuples match any of the configured per-column glob patterns, minus any
 * groups matched by an entry in {@code excludePatterns}. Each pattern is a {@code Map<String, String>} where keys are
 * clustering column names (or operator-side virtual column names, see below) and values are glob patterns matched
 * against the rendered tuple value at that column. Columns omitted from a pattern are treated as wildcards. Glob
 * syntax (including escape semantics) is shared with {@link WildcardProjectionPartialLoadMatcher} via {@link Globs}.
 * <p>
 * If the operator supplies {@link #getVirtualColumns()}, a pattern key may also reference one of those virtual
 * columns. At match time, the matcher resolves such a key to a clustering column on the segment via
 * {@link VirtualColumns#findEquivalent(VirtualColumns.Node)} between the matchers VCs and the segment's clustering
 * VCs (carried on {@link ClusterGroupTuples#virtualColumns()}). This lets operators author portable rules, they
 * write their preferred VC name and expression, and the matcher resolves to whatever name the segment happens to use
 * for the equivalent clustering VC.
 * <p>
 * If the operator-side VC for a pattern key has no equivalent clustering VC on the segment, the pattern is treated as
 * non-matching for that segment (defensive against typos or schema drift). The operator-VC-first ordering also
 * disambiguates the shadowing case where an operator-VC and a clustering column share a name: the operator-VC
 * interpretation wins, and a pattern is only matchable when the VCs are actually equivalent.
 * <p>
 * Null clustering values are matched only by omitting the column entirely or using the literal {@code "*"} glob; both
 * have explicit "match-anything-including-null" semantics. Any other glob (including {@code "null"}) does not match a
 * null clustering value. Matching specifically by null clustering values (e.g., load only the null-keyed group) is
 * not supported by this matcher and is left for a future typed-tuple matcher variant.
 */
public class WildcardClusterGroupPartialLoadMatcher extends ClusterGroupPartialLoadMatcher
{
  public static final String TYPE = "globClusterGroup";

  private final List<Map<String, String>> patterns;
  private final List<Map<String, String>> excludePatterns;
  private final VirtualColumns virtualColumns;
  private final List<Map<String, Globs.CompiledGlob>> compiledPatterns;
  private final List<Map<String, Globs.CompiledGlob>> compiledExcludePatterns;

  @JsonCreator
  public WildcardClusterGroupPartialLoadMatcher(
      @JsonProperty("patterns") List<Map<String, String>> patterns,
      @JsonProperty("excludePatterns") @Nullable List<Map<String, String>> excludePatterns,
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns
  )
  {
    if (patterns == null || patterns.isEmpty()) {
      throw InvalidInput.exception("patterns must not be null or empty for globClusterGroup matcher");
    }
    this.patterns = copyAndValidatePatterns(patterns, "patterns");
    this.excludePatterns = excludePatterns == null
                           ? List.of()
                           : copyAndValidatePatterns(excludePatterns, "excludePatterns");
    this.virtualColumns = internVirtualColumns(virtualColumns);
    this.compiledPatterns = compileAll(this.patterns);
    this.compiledExcludePatterns = compileAll(this.excludePatterns);
  }

  /**
   * Convenience constructor for callers that don't carry operator-side virtual columns. Equivalent to passing
   * {@code null} for the virtual columns argument.
   */
  public WildcardClusterGroupPartialLoadMatcher(
      List<Map<String, String>> patterns,
      @Nullable List<Map<String, String>> excludePatterns
  )
  {
    this(patterns, excludePatterns, null);
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

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @Override
  @Nullable
  protected List<Integer> resolveClusterGroupIndices(DataSegment segment)
  {
    final ClusterGroupTuples clusterGroups = segment.getClusterGroups();
    final RowSignature clusteringColumns = clusterGroups.clusteringColumns();
    final VirtualColumns segmentVcs = clusterGroups.virtualColumns();
    final List<List<Object>> tuples = clusterGroups.tuples();

    // Per-pattern resolution: which clustering column does each pattern key map to? Resolution is segment-scoped
    // (depends only on the segment's clustering signature + VCs), so compute it once up front and reuse it across
    // tuples. A null entry in the resolved list marks the pattern as non-matching for this segment.
    final List<Map<String, String>> resolvedPatterns = resolveAll(compiledPatterns, clusteringColumns, segmentVcs);
    final List<Map<String, String>> resolvedExcludes = resolveAll(compiledExcludePatterns, clusteringColumns, segmentVcs);

    // Compatibility check: at least one pattern must be fully resolvable against this segment's clustering scheme
    // for the matcher to have any meaningful opinion about the segment. If none of the patterns resolve, the
    // matcher's columns/VCs don't intersect what the segment clusters on at all, so the matcher is opaque to this
    // segment and the base class will fall back to the rule's cannot-match handling.
    boolean anyPatternResolved = false;
    for (Map<String, String> resolved : resolvedPatterns) {
      if (resolved != null) {
        anyPatternResolved = true;
        break;
      }
    }
    if (!anyPatternResolved) {
      return null;
    }

    final TreeSet<Integer> matched = new TreeSet<>();
    for (int i = 0; i < tuples.size(); i++) {
      final List<Object> tuple = tuples.get(i);
      if (!matchesAnyPattern(tuple, clusteringColumns, compiledPatterns, resolvedPatterns)) {
        continue;
      }
      if (matchesAnyPattern(tuple, clusteringColumns, compiledExcludePatterns, resolvedExcludes)) {
        continue;
      }
      matched.add(i);
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
    WildcardClusterGroupPartialLoadMatcher that = (WildcardClusterGroupPartialLoadMatcher) o;
    return Objects.equals(patterns, that.patterns)
           && Objects.equals(excludePatterns, that.excludePatterns)
           && Objects.equals(virtualColumns, that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(patterns, excludePatterns, virtualColumns);
  }

  @Override
  public String toString()
  {
    return "WildcardClusterGroupPartialLoadMatcher{patterns=" + patterns
           + ", excludePatterns=" + excludePatterns
           + ", virtualColumns=" + virtualColumns + "}";
  }

  /**
   * For each compiled pattern, compute the {@code patternKey -> segment clustering column name} map. A {@code null}
   * entry in the returned list marks a pattern as non-matching for this segment (some pattern key couldn't be
   * resolved to a clustering column via either direct name or operator-VC equivalence).
   */
  private List<Map<String, String>> resolveAll(
      List<Map<String, Globs.CompiledGlob>> compiled,
      RowSignature clusteringColumns,
      VirtualColumns segmentVcs
  )
  {
    if (compiled.isEmpty()) {
      return List.of();
    }
    final List<Map<String, String>> out = new ArrayList<>(compiled.size());
    for (Map<String, Globs.CompiledGlob> pattern : compiled) {
      out.add(resolvePattern(pattern.keySet(), clusteringColumns, segmentVcs));
    }
    return out;
  }

  @Nullable
  private Map<String, String> resolvePattern(
      Set<String> patternKeys,
      RowSignature clusteringColumns,
      VirtualColumns segmentVcs
  )
  {
    final Map<String, String> resolved = CollectionUtils.newLinkedHashMapWithExpectedSize(patternKeys.size());
    for (String key : patternKeys) {
      final String target = resolveKey(key, clusteringColumns, segmentVcs);
      if (target == null) {
        return null;   // pattern unresolvable for this segment
      }
      resolved.put(key, target);
    }
    return resolved;
  }

  /**
   * Resolve a pattern key to a clustering column name on the segment. Three cases:
   * <ol>
   *   <li>The matcher carries an virtual column by this name, resolve via
   *       {@link VirtualColumns#findEquivalent(VirtualColumns.Node)} against the segment's clustering VCs. The
   *       matcher VC interpretation wins regardless of any same-name clustering column (shadowing).</li>
   *   <li>Otherwise, if the key is directly a clustering column name -> identity.</li>
   *   <li>Otherwise -> unresolvable (returns {@code null}).</li>
   * </ol>
   */
  @Nullable
  private String resolveKey(String key, RowSignature clusteringColumns, VirtualColumns segmentVcs)
  {
    final VirtualColumns.Node operatorVcNode = virtualColumns.getNode(key);
    if (operatorVcNode != null) {
      final VirtualColumn equivalent = segmentVcs.findEquivalent(operatorVcNode);
      if (equivalent == null) {
        return null;
      }
      final String equivalentName = equivalent.getOutputName();
      return clusteringColumns.contains(equivalentName) ? equivalentName : null;
    }
    return clusteringColumns.contains(key) ? key : null;
  }

  private static boolean matchesAnyPattern(
      List<Object> tuple,
      RowSignature clusteringColumns,
      List<Map<String, Globs.CompiledGlob>> compiledPatterns,
      List<Map<String, String>> resolvedPatterns
  )
  {
    for (int p = 0; p < compiledPatterns.size(); p++) {
      final Map<String, String> resolved = resolvedPatterns.get(p);
      if (resolved == null) {
        continue;
      }
      if (matchesPattern(tuple, clusteringColumns, compiledPatterns.get(p), resolved)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesPattern(
      List<Object> tuple,
      RowSignature clusteringColumns,
      Map<String, Globs.CompiledGlob> pattern,
      Map<String, String> resolved
  )
  {
    for (Map.Entry<String, Globs.CompiledGlob> entry : pattern.entrySet()) {
      final String resolvedColumn = resolved.get(entry.getKey());
      final int idx = clusteringColumns.indexOf(resolvedColumn);
      // resolved is guaranteed to map every patternKey to a real clustering column (else the pattern was skipped).
      final Globs.CompiledGlob glob = entry.getValue();
      if (glob.isMatchAny()) {
        // Literal "*" matches every value, including null.
        continue;
      }
      final Object value = tuple.get(idx);
      if (value == null) {
        // Any non-"*" glob cannot match a null clustering value.
        return false;
      }
      if (!glob.matches(value.toString())) {
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

  private static List<Map<String, Globs.CompiledGlob>> compileAll(List<Map<String, String>> patterns)
  {
    if (patterns.isEmpty()) {
      return List.of();
    }
    final List<Map<String, Globs.CompiledGlob>> out = new ArrayList<>(patterns.size());
    for (Map<String, String> pattern : patterns) {
      final Map<String, Globs.CompiledGlob> compiled = CollectionUtils.newLinkedHashMapWithExpectedSize(pattern.size());
      for (Map.Entry<String, String> entry : pattern.entrySet()) {
        compiled.put(entry.getKey(), Globs.compile(entry.getValue()));
      }
      out.add(Collections.unmodifiableMap(compiled));
    }
    return List.copyOf(out);
  }

  private static VirtualColumns internVirtualColumns(@Nullable VirtualColumns virtualColumns)
  {
    if (virtualColumns == null || virtualColumns.isEmpty()) {
      return VirtualColumns.EMPTY;
    }
    return VirtualColumns.create(
        Arrays.stream(virtualColumns.getVirtualColumns())
              .map(DataSegment.virtualColumnInterner()::intern)
              .toList()
    );
  }
}
