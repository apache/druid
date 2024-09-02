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
import org.apache.druid.query.datasourcemetadata.DataSourceMetadataResultValue;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.List;

/**
 * The difference between this class and {@link org.apache.druid.timeline.DataSegment} is that {@link
 * org.apache.druid.timeline.DataSegment} contains the segment metadata only, while this class represents the actual
 * body of segment data, queryable.
 */
@PublicApi
public interface Segment extends Closeable
{
  SegmentId getId();

  Interval getDataInterval();

  @Nullable
  QueryableIndex asQueryableIndex();

  @Deprecated
  default StorageAdapter asStorageAdapter()
  {
    throw DruidException.defensive(
        "asStorageAdapter is no longer supported, use Segment.asCursorFactory to build cursors, or Segment.as(..) to get various metadata information instead"
    );
  }

  default CursorFactory asCursorFactory()
  {
    // todo (clint): should we just tear off the bandaid and make everyone implement this?
    final StorageAdapter storageAdapter = asStorageAdapter();
    return new CursorFactory()
    {
      @Override
      public CursorHolder makeCursorHolder(CursorBuildSpec spec)
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
            return storageAdapter.canVectorize(
                spec.getFilter(),
                spec.getVirtualColumns(),
                descending
            );
          }

          @Override
          public Cursor asCursor()
          {
            return Iterables.getOnlyElement(
                storageAdapter.makeCursors(
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
            return storageAdapter.makeVectorCursor(
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

      @Override
      public RowSignature getRowSignature()
      {
        return storageAdapter.getRowSignature();
      }

      @Override
      @Nullable
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return storageAdapter.getColumnCapabilities(column);
      }
    };
  }

  /**
   * Request an implementation of a particular interface.
   *
   * If the passed-in interface is {@link QueryableIndex} or {@link CursorFactory}, then this method behaves
   * identically to {@link #asQueryableIndex()} or {@link #asCursorFactory()} ()}. Other interfaces are only
   * expected to be requested by callers that have specific knowledge of extra features provided by specific
   * segment types. For example, an extension might provide a custom Segment type that can offer both
   * StorageAdapter and some new interface. That extension can also offer a Query that uses that new interface.
   *
   * Implementations which accept classes other than {@link QueryableIndex} or {@link CursorFactory} are limited
   * to using those classes within the extension. This means that one extension cannot rely on the `Segment.as`
   * behavior of another extension.
   *
   * @param clazz desired interface
   * @param <T>   desired interface
   *
   * @return instance of clazz, or null if the interface is not supported by this segment
   *
   * @see CursorFactory to make cursors to run queries
   * @see QueryableIndex index object, if this is a memory-mapped regular segment.
   * @see IndexedTable table object, if this is a joinable indexed table.
   * @see TimeBoundaryInspector inspector for min/max timestamps, if supported by this segment.
   * @see MaxIngestedEventTimeInspector inspector for {@link DataSourceMetadataResultValue#getMaxIngestedEventTime()}
   * @see PhysicalSegmentInspector inspector for {@link org.apache.druid.query.metadata.SegmentAnalyzer}
   * @see Metadata information about how a physical segment was created
   * @see TopNOptimizationInspector inspector containing information for topN specific  optimizations
   * @see CloseableShapeshifter stepping stone to {@link org.apache.druid.query.rowsandcols.RowsAndColumns}.
   */
  @SuppressWarnings({"unused", "unchecked"})
  @Nullable
  default <T> T as(@Nonnull Class<T> clazz)
  {
    if (clazz.equals(CursorFactory.class)) {
      return (T) asCursorFactory();
    } else if (clazz.equals(QueryableIndex.class)) {
      return (T) asQueryableIndex();
    } else if (clazz.equals(StorageAdapter.class)) {
      return (T) asStorageAdapter();
    }
    return null;
  }

  default boolean isTombstone()
  {
    return false;
  }


  default String asString()
  {
    return getClass().toString();
  }
}
