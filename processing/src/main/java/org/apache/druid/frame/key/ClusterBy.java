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

package org.apache.druid.frame.key;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.frame.FrameType;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Describes a key used for sorting or partitioning.
 *
 * Keys have columns, and some of those columns may comprise a "bucket key". See {@link #getBucketByCount()} for
 * details about bucket keys.
 */
public class ClusterBy
{
  private final List<KeyColumn> columns;
  private final int bucketByCount;
  private final boolean sortable;

  @JsonCreator
  public ClusterBy(
      @JsonProperty("columns") List<KeyColumn> columns,
      @JsonProperty("bucketByCount") int bucketByCount
  )
  {
    this.columns = Preconditions.checkNotNull(columns, "columns");
    this.bucketByCount = bucketByCount;

    if (bucketByCount < 0 || bucketByCount > columns.size()) {
      throw new IAE("Invalid bucketByCount [%d]", bucketByCount);
    }

    // Key must be 100% sortable or 100% nonsortable. If empty, call it sortable.
    boolean sortable = true;

    for (int i = 0; i < columns.size(); i++) {
      final KeyColumn column = columns.get(i);

      if (i == 0) {
        sortable = column.order().sortable();
      } else if (sortable != column.order().sortable()) {
        throw new IAE("Cannot mix sortable and unsortable key columns");
      }
    }

    this.sortable = sortable;
  }

  /**
   * Create an empty key.
   */
  public static ClusterBy none()
  {
    return new ClusterBy(Collections.emptyList(), 0);
  }

  /**
   * The columns that comprise this key, in order.
   */
  @JsonProperty
  public List<KeyColumn> getColumns()
  {
    return columns;
  }

  /**
   * How many fields from {@link #getColumns()} comprise the "bucket key". Bucketing is like strict partitioning: all
   * rows in a given partition will have the exact same bucket key. It is most commonly used to implement
   * segment granularity during ingestion.
   *
   * The bucket key is a prefix of the complete key.
   *
   * Will always be less than, or equal to, the size of {@link #getColumns()}.
   *
   * Only relevant when a ClusterBy instance is used as a partitioning key.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getBucketByCount()
  {
    return bucketByCount;
  }

  /**
   * Whether this key is empty.
   */
  public boolean isEmpty()
  {
    return columns.isEmpty();
  }

  /**
   * Whether this key is sortable. Empty keys (with no columns) are considered sortable.
   */
  public boolean sortable()
  {
    return sortable;
  }

  /**
   * Create a reader for keys for this instance.
   *
   * The provided {@link ColumnInspector} is used to determine the types of fields in the keys. The provided signature
   * does not have to exactly match the sortColumns: it merely has to contain them all.
   */
  public RowKeyReader keyReader(final ColumnInspector inspector, final FrameType frameType)
  {
    final RowSignature.Builder newSignature = RowSignature.builder();

    for (final KeyColumn keyColumn : columns) {
      final String columnName = keyColumn.columnName();
      final ColumnCapabilities capabilities = inspector.getColumnCapabilities(columnName);
      final ColumnType columnType =
          Preconditions.checkNotNull(capabilities, "Type for column [%s]", columnName).toColumnType();

      newSignature.add(columnName, columnType);
    }

    return RowKeyReader.create(newSignature.build(), frameType);
  }

  /**
   * Comparator that compares keys for this instance using the given signature.
   */
  public Comparator<RowKey> keyComparator(RowSignature rowSignature)
  {
    return RowKeyComparator.create(columns, rowSignature);
  }

  /**
   * Comparator that compares byte arrays of keys for this instance using the given signature directly.
   */
  public Comparator<byte[]> byteKeyComparator(RowSignature rowSignature)
  {
    return ByteRowKeyComparator.create(columns, rowSignature);
  }

  /**
   * Comparator that compares bucket keys for this instance. Bucket keys are retrieved by calling
   * {@link RowKeyReader#trim(RowKey, int)} with {@link #getBucketByCount()}.
   */
  public Comparator<RowKey> bucketComparator(final RowSignature rowSignature)
  {
    if (bucketByCount == 0) {
      return Comparators.alwaysEqual();
    } else {
      return RowKeyComparator.create(columns.subList(0, bucketByCount), rowSignature);
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
    ClusterBy clusterBy = (ClusterBy) o;
    return bucketByCount == clusterBy.bucketByCount && Objects.equals(columns, clusterBy.columns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columns, bucketByCount);
  }

  @Override
  public String toString()
  {
    return "ClusterBy{" +
           "columns=" + columns +
           ", bucketByCount=" + bucketByCount +
           '}';
  }
}
