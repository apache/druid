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

package org.apache.druid.segment.projections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A single cluster group within a clustered base table, with all rows sharing the clustering-column tuple given by
 * {@link #lookupClusteringValues()}.
 * <p/>
 * Shape is very minimal containing only clustering value ids and row count; other things such as columns, ordering,
 * dictionaries, etc. live on the parent {@link ClusteredValueGroupsBaseTableSchema} as they are identical from group
 * to group within a segment. The parent summary is attached after construction via {@link #setSummary} and reached
 * via {@link #getSummary}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public class TableClusterGroupSpec
{
  public static final String TYPE_NAME = "cluster-base-table";

  /**
   * Clustering value id tuple (positions in the parent summary's per-column dictionaries).
   */
  private final List<Integer> clusteringValueIds;

  /**
   * Number of rows in this cluster group's per-group column data.
   */
  private final int numRows;

  /**
   * Parent back-reference, set once via {@link #setSummary} during the schema constructor. Readers observe the
   * spec only after the holding {@code SimpleQueryableIndex}'s final field publishes it, so non-final is safe.
   */
  @Nullable
  private ClusteredValueGroupsBaseTableSchema summary;

  @JsonCreator
  public TableClusterGroupSpec(
      @JsonProperty("clusteringValueIds") List<Integer> clusteringValueIds,
      @JsonProperty("numRows") @Nullable Integer numRows
  )
  {
    if (clusteringValueIds == null) {
      throw DruidException.defensive("clusteringValueIds must not be null");
    }
    this.clusteringValueIds = List.copyOf(clusteringValueIds);
    this.numRows = numRows == null ? 0 : numRows;
    this.summary = null;
  }

  /**
   * Wire up the parent summary. Called once per spec by {@link ClusteredValueGroupsBaseTableSchema}'s constructor.
   */
  public void setSummary(ClusteredValueGroupsBaseTableSchema summary)
  {
    DruidException.conditionalDefensive(summary != null, "summary must not be null");
    DruidException.conditionalDefensive(this.summary == null, "summary already set");

    final RowSignature clusteringColumns = summary.getClusteringColumns();
    final int numCols = clusteringColumns.size();

    DruidException.conditionalDefensive(
        clusteringValueIds.size() == numCols,
        "clusteringValueIds size [%s] does not match summary clusteringColumns size [%s]",
        clusteringValueIds.size(),
        numCols
    );

    this.summary = summary;
  }

  /**
   * Returns the parent summary. Throws if {@link #setSummary} hasn't been called yet.
   */
  public ClusteredValueGroupsBaseTableSchema getSummary()
  {
    DruidException.conditionalDefensive(
        this.summary != null,
        "TableClusterGroupSpec.setSummary must be called before this method"
    );
    return summary;
  }

  /**
   * Typed clustering values for this group, materialized fresh from the summary's dictionaries on each call.
   * Length and order match {@link ClusteredValueGroupsBaseTableSchema#getClusteringColumns()}. Callers that walk
   * the array repeatedly should cache the returned reference.
   */
  public Object[] lookupClusteringValues()
  {
    final ClusteredValueGroupsBaseTableSchema s = getSummary();
    final int numCols = clusteringValueIds.size();
    final Object[] out = new Object[numCols];
    for (int i = 0; i < numCols; i++) {
      out[i] = s.lookupClusteringValue(i, clusteringValueIds.get(i));
    }
    return out;
  }

  /**
   * Dictionary ids identifying this group, one per clustering column.
   */
  @JsonProperty
  public List<Integer> getClusteringValueIds()
  {
    return clusteringValueIds;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getNumRows()
  {
    return numRows;
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
    TableClusterGroupSpec that = (TableClusterGroupSpec) o;
    // Specs are only compared within a single segment context, where segment-local IDs uniquely identify a tuple.
    return Objects.equals(clusteringValueIds, that.clusteringValueIds);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(clusteringValueIds);
  }

  @Override
  public String toString()
  {
    if (summary == null) {
      return "TableClusterGroupSpec{" +
             "clusteringValues=" + "<unset, ids=" + clusteringValueIds + ">" +
             '}';
    }
    return "TableClusterGroupSpec{" +
           "clusteringValues=" + Arrays.toString(lookupClusteringValues()) +
           '}';
  }
}
