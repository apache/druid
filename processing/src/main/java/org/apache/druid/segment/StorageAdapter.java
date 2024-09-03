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

package org.apache.druid.segment;

import com.google.common.collect.Iterables;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

/**
 *
 */
@PublicApi
public interface StorageAdapter extends CursorFactory, ColumnInspector, CursorHolderFactory
{

  /**
   * Build a {@link CursorHolder} which can provide {@link Cursor} and {@link VectorCursor} (if capable) which allows
   * scanning segments and creating {@link ColumnSelectorFactory} and
   * {@link org.apache.druid.segment.vector.VectorColumnSelectorFactory} respectively to read row values at the cursor
   * position.
   */
  @Override
  default CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    // For backwards compatibility, the default implementation assumes the underlying rows are sorted by __time.
    // Built-in implementations of StorageAdapter must override this method.
    final List<OrderBy> ordering;
    final boolean descending;
    if (Cursors.preferDescendingTimeOrdering(spec)) {
      ordering = Cursors.descendingTimeOrder();
      descending = true;
    } else {
      ordering = Cursors.ascendingTimeOrder();
      descending = false;
    }
    return new CursorHolder()
    {
      @Override
      public boolean canVectorize()
      {
        return StorageAdapter.this.canVectorize(
            spec.getFilter(),
            spec.getVirtualColumns(),
            descending
        );
      }

      @Override
      public Cursor asCursor()
      {
        return Iterables.getOnlyElement(
            StorageAdapter.this.makeCursors(
                spec.getFilter(),
                spec.getInterval(),
                spec.getVirtualColumns(),
                Granularities.ALL,
                descending,
                spec.getQueryMetrics()
            ).toList()
        );
      }

      @Override
      public VectorCursor asVectorCursor()
      {
        return StorageAdapter.this.makeVectorCursor(
            spec.getFilter(),
            spec.getInterval(),
            spec.getVirtualColumns(),
            descending,
            spec.getQueryContext().getVectorSize(),
            spec.getQueryMetrics()
        );
      }

      @Nullable
      @Override
      public List<OrderBy> getOrdering()
      {
        return ordering;
      }

      @Override
      public void close()
      {
        // consuming sequences of CursorFactory are expected to close themselves.
      }
    };
  }

  Interval getInterval();

  /**
   * Returns the names of all dimension columns, not including {@link ColumnHolder#TIME_COLUMN_NAME}.
   */
  Indexed<String> getAvailableDimensions();

  /**
   * Returns the names of all metric columns.
   */
  Iterable<String> getAvailableMetrics();

  /**
   * Returns the row signature of the data available from this adapter. For mutable adapters, even though the signature
   * may evolve over time, any particular object returned by this method is an immutable snapshot.
   */
  default RowSignature getRowSignature()
  {
    final RowSignature.Builder builder = RowSignature.builder();
    builder.addTimeColumn();

    for (final String column : Iterables.concat(getAvailableDimensions(), getAvailableMetrics())) {
      builder.add(column, ColumnType.fromCapabilities(getColumnCapabilities(column)));
    }

    return builder.build();
  }

  /**
   * Returns the number of distinct values for a column, or {@link DimensionDictionarySelector#CARDINALITY_UNKNOWN}
   * if unknown.
   *
   * If the column doesn't exist, returns 1, because a column that doesn't exist is treated as a column of default
   * (or null) values.
   */
  int getDimensionCardinality(String column);

  /**
   * Use {@link TimeBoundaryInspector#getMinTime()} instead.
   */
  @Deprecated
  default DateTime getMinTime()
  {
    throw DruidException.defensive(
        "getMinTime is no longer supported, use Segment.as(MinMaxValueInspector.class) instead"
    );
  }

  /**
   * Use {@link TimeBoundaryInspector#getMaxTime()} instead.
   */
  @Deprecated
  default DateTime getMaxTime()
  {
    throw DruidException.defensive(
        "getMaxTime is no longer supported, use Segment.as(MinMaxValueInspector.class) instead"
    );
  }

  /**
   * Returns the minimum value of the provided column, if known through an index, dictionary, or cache. Returns null
   * if not known. Does not scan the column to find the minimum value.
   */
  @Nullable
  Comparable getMinValue(String column);

  /**
   * Returns the minimum value of the provided column, if known through an index, dictionary, or cache. Returns null
   * if not known. Does not scan the column to find the maximum value.
   */
  @Nullable
  Comparable getMaxValue(String column);

  /**
   * Returns capabilities of a particular column, if known. May be null if the column doesn't exist, or if
   * the column does exist but the capabilities are unknown. The latter is possible with dynamically discovered
   * columns.
   *
   * Note that StorageAdapters are representations of "real" segments, so they are not aware of any virtual columns
   * that may be involved in a query. In general, query engines should instead use the method
   * {@link ColumnSelectorFactory#getColumnCapabilities(String)}, which returns capabilities for virtual columns as
   * well.
   *
   * @param column column name
   *
   * @return capabilities, or null
   */
  @Override
  @Nullable
  ColumnCapabilities getColumnCapabilities(String column);

  int getNumRows();

  /**
   * Use {@link MaxIngestedEventTimeInspector#getMaxIngestedEventTime()} instead.
   */
  @Deprecated
  default DateTime getMaxIngestedEventTime()
  {
    throw DruidException.defensive(
        "getMaxIngestedEventTime is no longer supported, use Segment.as(MaxIngestedEventTimeInspector.class) instead"
    );
  }

  @Nullable
  Metadata getMetadata();

  /**
   * Returns true if this storage adapter can filter some rows out. The actual column cardinality can be lower than
   * what {@link #getDimensionCardinality} returns if this returns true. Dimension selectors for such storage adapter
   * can return non-contiguous dictionary IDs because the dictionary IDs in filtered rows will not be returned.
   * Note that the number of rows accessible via this storage adapter will not necessarily decrease because of
   * the built-in filters. For inner joins, for example, the number of joined rows can be larger than
   * the number of rows in the base adapter even though this method returns true.
   */
  default boolean hasBuiltInFilters()
  {
    return false;
  }

  /**
   * @return true if this index was created from a tombstone or false otherwise
   */
  default boolean isFromTombstone()
  {
    return false;
  }
}
