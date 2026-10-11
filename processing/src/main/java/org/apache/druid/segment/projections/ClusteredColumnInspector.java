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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Inspector for segment-wide column capabilities of a clustered base table, computed from the
 * {@link ColumnDescriptor} in the segment file metadata.
 */
public class ClusteredColumnInspector implements ColumnInspector
{
  private final ClusteredValueGroupsBaseTableSchema summary;

  // computed on first use, since it takes one descriptor lookup per (group, column) pair
  private final Supplier<Map<String, ColumnCapabilities>> groupColumnCapabilities;

  public ClusteredColumnInspector(
      ClusteredValueGroupsBaseTableSchema summary,
      Map<String, ColumnDescriptor> columnDescriptors
  )
  {
    this.summary = summary;
    this.groupColumnCapabilities = Suppliers.memoize(() -> computeGroupColumnCapabilities(summary, columnDescriptors));
  }

  /**
   * Returns a {@link ClusteredColumnInspector} instance if the {@link ClusteredValueGroupsBaseTableSchema}
   * is nonnull; returns null otherwise.
   */
  @Nullable
  public static ClusteredColumnInspector create(
      @Nullable ClusteredValueGroupsBaseTableSchema summary,
      Map<String, ColumnDescriptor> columnDescriptors
  )
  {
    return summary == null ? null : new ClusteredColumnInspector(summary, columnDescriptors);
  }

  /**
   * Returns capabilities of a clustering column, which is constant within each cluster group.
   */
  public static ColumnCapabilities clusteringColumnCapabilities(ColumnType type)
  {
    return type.is(ValueType.STRING)
           ? ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
           : ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(type);
  }

  /**
   * Returns capabilities of a column from its {@link ColumnDescriptor}. Descriptors record the type and
   * whether there are multiple values, but not whether there are nulls, so {@link ColumnCapabilities#hasNulls()} is
   * unknown for every column except {@code __time}.
   */
  public static ColumnCapabilities capabilitiesFromDescriptor(String column, ColumnDescriptor descriptor)
  {
    return ColumnCapabilitiesImpl.createDefault()
                                 .setType(descriptor.toColumnType())
                                 .setHasMultipleValues(descriptor.isHasMultipleValues())
                                 .setHasNulls(
                                     ColumnHolder.TIME_COLUMN_NAME.equals(column)
                                     ? ColumnCapabilities.Capable.FALSE
                                     : ColumnCapabilities.Capable.UNKNOWN
                                 );
  }

  /**
   * Returns capabilities of a column that has capabilities {@code a} in some cluster groups and {@code b} in others:
   * the least restrictive of the two types, with multiple values or nulls if either has them. Dictionary flags are
   * false, since each group has its own dictionary.
   */
  public static ColumnCapabilities combineGroupCapabilities(String column, ColumnCapabilities a, ColumnCapabilities b)
  {
    final ColumnType type;
    try {
      type = ColumnType.leastRestrictiveType(a.toColumnType(), b.toColumnType());
    }
    catch (Types.IncompatibleTypeException e) {
      throw DruidException.defensive(e, "Column[%s] has incompatible types in different cluster groups", column);
    }
    return ColumnCapabilitiesImpl.createDefault()
                                 .setType(type)
                                 .setHasMultipleValues(a.hasMultipleValues().or(b.hasMultipleValues()))
                                 .setHasNulls(a.hasNulls().or(b.hasNulls()));
  }

  /**
   * Returns capabilities of a logical column of the clustered table, or null if the table has no such column.
   * Clustering columns resolve from the summary's typed clustering signature, all others from the cluster groups'
   * descriptors.
   */
  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    final ColumnType clusteringType = summary.getClusteringColumns().getColumnType(column).orElse(null);
    if (clusteringType != null) {
      return clusteringColumnCapabilities(clusteringType);
    }
    return groupColumnCapabilities.get().get(column);
  }

  private static Map<String, ColumnCapabilities> computeGroupColumnCapabilities(
      ClusteredValueGroupsBaseTableSchema summary,
      Map<String, ColumnDescriptor> columnDescriptors
  )
  {
    final List<TableClusterGroupSpec> groups = summary.getClusterGroups();
    final List<String> groupPrefixes = new ArrayList<>(groups.size());
    for (TableClusterGroupSpec group : groups) {
      groupPrefixes.add(Projections.getClusterGroupSegmentInternalFilePrefix(group.getClusteringValueIds()));
    }

    final List<String> columns = summary.getGroupColumnNames();
    final Map<String, ColumnCapabilities> capabilities = Maps.newHashMapWithExpectedSize(columns.size());
    for (String column : columns) {
      ColumnCapabilities combined = null;
      for (String groupPrefix : groupPrefixes) {
        final ColumnDescriptor descriptor = columnDescriptors.get(groupPrefix + column);
        if (descriptor != null) {
          final ColumnCapabilities groupCapabilities = capabilitiesFromDescriptor(column, descriptor);
          combined = combined == null
                     ? groupCapabilities
                     : combineGroupCapabilities(column, combined, groupCapabilities);
        }
      }

      if (combined != null) {
        capabilities.put(column, combined);
      }
    }
    return capabilities;
  }
}
