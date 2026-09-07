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

package org.apache.druid.msq.querykit.groupby;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.processor.FrameCombiner;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.TrackingColumnValueSelector;
import org.apache.druid.frame.processor.TrackingDimensionSelector;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameCursor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link FrameCombiner} for groupBy queries. Combines aggregate values for rows with
 * identical dimension keys using {@link AggregatorFactory#combine}.
 */
public class GroupByFrameCombiner implements FrameCombiner
{
  private final RowSignature signature;
  private final int aggregatorStart;
  private final Object[] aggregateValues;
  private final List<AggregatorFactory> aggregatorFactories;
  private final CombinedColumnSelectorFactory combinedColumnSelectorFactory;

  private FrameReader frameReader;

  /**
   * Frame for {@link #cachedCursor} and {@link #cachedAggregatorSelectors}. Updated by {@link #getCursor(Frame)}.
   */
  @Nullable
  private Frame cachedFrame;

  /**
   * Cached cursor for the current frame. Updated by {@link #getCursor(Frame)}.
   */
  @Nullable
  private FrameCursor cachedCursor;

  /**
   * Cached aggregator selectors for the current frame. Updated by {@link #getCursor(Frame)}.
   * Same length as {@link #aggregatorFactories}.
   */
  @Nullable
  private ColumnValueSelector<?>[] cachedAggregatorSelectors;

  public GroupByFrameCombiner(
      final RowSignature signature,
      final List<AggregatorFactory> aggregatorFactories,
      final int aggregatorStart
  )
  {
    this.signature = signature;
    this.aggregatorStart = aggregatorStart;
    this.aggregatorFactories = aggregatorFactories;
    this.aggregateValues = new Object[aggregatorFactories.size()];
    this.combinedColumnSelectorFactory = new CombinedColumnSelectorFactory();
  }

  @Override
  public void init(final FrameReader frameReader)
  {
    this.frameReader = frameReader;
  }

  @Override
  public void reset(final Frame frame, final int row)
  {
    final FrameCursor cursor = getCursor(frame);
    cursor.setCurrentRow(row);

    // Read aggregate values from this row using cached selectors.
    for (int i = 0; i < aggregatorFactories.size(); i++) {
      aggregateValues[i] = cachedAggregatorSelectors[i].getObject();
    }
  }

  @Override
  public void combine(final Frame frame, final int row)
  {
    final FrameCursor cursor = getCursor(frame);
    cursor.setCurrentRow(row);

    // Read and combine aggregate values using cached selectors.
    for (int i = 0; i < aggregatorFactories.size(); i++) {
      final Object newValue = cachedAggregatorSelectors[i].getObject();
      aggregateValues[i] = aggregatorFactories.get(i).combine(aggregateValues[i], newValue);
    }
  }

  @Override
  public ColumnSelectorFactory getCombinedColumnSelectorFactory()
  {
    return combinedColumnSelectorFactory;
  }

  /**
   * Returns a cursor for the given frame, reusing a cached cursor if the frame has not changed.
   * Also rebuilds {@link #cachedAggregatorSelectors} when the cursor changes.
   */
  private FrameCursor getCursor(final Frame frame)
  {
    //noinspection ObjectEquality
    if (frame != cachedFrame) {
      cachedFrame = frame;
      cachedCursor = FrameProcessors.makeCursor(frame, frameReader);

      // Rebuild aggregator selectors for the new cursor.
      final ColumnSelectorFactory columnSelectorFactory = cachedCursor.getColumnSelectorFactory();
      cachedAggregatorSelectors = new ColumnValueSelector<?>[aggregatorFactories.size()];
      for (int i = 0; i < aggregatorFactories.size(); i++) {
        cachedAggregatorSelectors[i] =
            columnSelectorFactory.makeColumnValueSelector(signature.getColumnName(aggregatorStart + i));
      }
    }
    return cachedCursor;
  }

  /**
   * ColumnSelectorFactory that reads dimension columns from the cached frame cursor, and aggregate columns from
   * {@link #aggregateValues}. Key columns can be read from any row in the current group, since
   * all rows in a group share the same key. The cached cursor is always positioned at the most recent row passed to
   * {@link #reset} or {@link #combine}.
   */
  private class CombinedColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final Map<String, TrackingColumnValueSelector> columnValueSelectorCache = new HashMap<>();
    private final Map<DimensionSpec, TrackingDimensionSelector> dimensionSelectorCache = new HashMap<>();

    @Override
    public DimensionSelector makeDimensionSelector(final DimensionSpec dimensionSpec)
    {
      final int columnIndex = signature.indexOf(dimensionSpec.getDimension());

      if (columnIndex < 0) {
        return DimensionSelector.constant(null, dimensionSpec.getExtractionFn());
      } else if (columnIndex >= aggregatorStart && columnIndex < aggregatorStart + aggregatorFactories.size()) {
        // Aggregate column: return combined value as a single-valued DimensionSelector.
        final int aggIndex = columnIndex - aggregatorStart;
        return new BaseSingleValueDimensionSelector()
        {
          @Nullable
          @Override
          protected String getValue()
          {
            final Object val = aggregateValues[aggIndex];
            return val == null ? null : String.valueOf(val);
          }

          @Override
          public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
          {
            // Do nothing.
          }
        };
      } else {
        // Dimension: delegate to a cursor-tracking selector that refreshes when cachedCursor changes.
        return dimensionSelectorCache.computeIfAbsent(
            dimensionSpec,
            spec -> new TrackingDimensionSelector(spec, () -> cachedCursor.getColumnSelectorFactory())
        );
      }
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(final String columnName)
    {
      final int columnIndex = signature.indexOf(columnName);

      if (columnIndex < 0) {
        return NilColumnValueSelector.instance();
      } else if (columnIndex >= aggregatorStart && columnIndex < aggregatorStart + aggregatorFactories.size()) {
        // Aggregate column: return combined value as a ColumnValueSelector.
        final int aggIndex = columnIndex - aggregatorStart;
        return new ColumnValueSelector<>()
        {
          @Override
          public double getDouble()
          {
            return ((Number) aggregateValues[aggIndex]).doubleValue();
          }

          @Override
          public float getFloat()
          {
            return ((Number) aggregateValues[aggIndex]).floatValue();
          }

          @Override
          public long getLong()
          {
            return ((Number) aggregateValues[aggIndex]).longValue();
          }

          @Override
          public boolean isNull()
          {
            return aggregateValues[aggIndex] == null;
          }

          @Nullable
          @Override
          public Object getObject()
          {
            return aggregateValues[aggIndex];
          }

          @Override
          public Class<?> classOfObject()
          {
            return Object.class;
          }

          @Override
          public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
          {
            // Do nothing.
          }
        };
      } else {
        // Dimension: delegate to a cursor-tracking selector that refreshes when cachedCursor changes.
        return columnValueSelectorCache.computeIfAbsent(
            columnName,
            name -> new TrackingColumnValueSelector(name, () -> cachedCursor.getColumnSelectorFactory())
        );
      }
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(final String column)
    {
      return signature.getColumnCapabilities(column);
    }
  }
}
