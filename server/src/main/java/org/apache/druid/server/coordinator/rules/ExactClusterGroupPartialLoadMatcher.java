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
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Selects cluster groups whose clustering tuples exactly match one of the configured tuples, by typed equality.
 * Tuples are positional (operator authors them in the segment's clustering-column order) and may include nulls. Each
 * operator-supplied tuple is coerced to the segment's clustering-column types at match time via
 * {@link ClusterGroupTuples#coerceValue}; coercion failures (e.g., a string in a LONG column) skip the offending
 * tuple for that segment rather than failing the rule. Tuples whose length doesn't match the segment's clustering
 * column count are likewise skipped — safe failure mode when segments with different clustering schemas appear under
 * the same rule.
 * <p/>
 * Complement to {@link WildcardClusterGroupPartialLoadMatcher}: use the wildcard form for pattern-based partial loads
 * and use this form when you need to load specific tuples verbatim, especially those containing null clustering
 * values which the wildcard form can't constrain.
 */
public class ExactClusterGroupPartialLoadMatcher extends ClusterGroupPartialLoadMatcher
{
  public static final String TYPE = "exactClusterGroup";

  private final List<List<Object>> tuples;

  @JsonCreator
  public ExactClusterGroupPartialLoadMatcher(@JsonProperty("tuples") List<List<Object>> tuples)
  {
    if (tuples == null || tuples.isEmpty()) {
      throw InvalidInput.exception("tuples must not be null or empty for exactClusterGroup matcher");
    }
    final List<List<Object>> copied = new ArrayList<>(tuples.size());
    for (int i = 0; i < tuples.size(); i++) {
      final List<Object> tuple = tuples.get(i);
      if (tuple == null) {
        throw InvalidInput.exception("tuples[%s] must not be null", i);
      }
      // ArrayList copy here (not List.copyOf) so nulls inside the tuple are preserved.
      copied.add(Collections.unmodifiableList(new ArrayList<>(tuple)));
    }
    this.tuples = Collections.unmodifiableList(copied);
  }

  @JsonProperty
  public List<List<Object>> getTuples()
  {
    return tuples;
  }

  @Override
  protected List<Integer> resolveClusterGroupIndices(DataSegment segment)
  {
    final ClusterGroupTuples clusterGroups = segment.getClusterGroups();
    if (clusterGroups == null) {
      return Collections.emptyList();
    }
    final RowSignature clusteringColumns = clusterGroups.getClusteringColumns();
    final int numCols = clusteringColumns.size();
    final List<List<Object>> segmentTuples = clusterGroups.getTuples();

    final TreeSet<Integer> matched = new TreeSet<>();
    for (List<Object> ruleTuple : tuples) {
      if (ruleTuple.size() != numCols) {
        // Length mismatch — operator's tuples are for a different clustering schema; no match against this segment.
        continue;
      }
      final List<Object> coerced = tryCoerce(ruleTuple, clusteringColumns);
      if (coerced == null) {
        continue;
      }
      for (int i = 0; i < segmentTuples.size(); i++) {
        if (segmentTuples.get(i).equals(coerced)) {
          matched.add(i);
        }
      }
    }
    return new ArrayList<>(matched);
  }

  /**
   * Coerce an operator-supplied tuple to the segment's clustering-column types. Returns null if any value can't be
   * coerced — caller treats that as "skip this tuple for this segment" rather than failing.
   */
  private static List<Object> tryCoerce(List<Object> ruleTuple, RowSignature clusteringColumns)
  {
    final int numCols = clusteringColumns.size();
    final Object[] out = new Object[numCols];
    for (int i = 0; i < numCols; i++) {
      final String name = clusteringColumns.getColumnName(i);
      final ColumnType type = clusteringColumns.getColumnType(i).orElse(null);
      if (type == null) {
        return null;
      }
      try {
        out[i] = ClusterGroupTuples.coerceValue(name, type, ruleTuple.get(i));
      }
      catch (DruidException e) {
        return null;
      }
    }
    return Arrays.asList(out);
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
    ExactClusterGroupPartialLoadMatcher that = (ExactClusterGroupPartialLoadMatcher) o;
    return Objects.equals(tuples, that.tuples);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tuples);
  }

  @Override
  public String toString()
  {
    return "ExactClusterGroupPartialLoadMatcher{tuples=" + tuples + "}";
  }
}
