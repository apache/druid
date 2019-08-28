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

package org.apache.druid.query.select;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.QueryRunnerHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.dimension.ColumnSelectorStrategy;
import org.apache.druid.query.dimension.ColumnSelectorStrategyFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class SelectQueryEngine
{
  private static final SelectStrategyFactory STRATEGY_FACTORY = new SelectStrategyFactory();

  public static class SelectStrategyFactory implements ColumnSelectorStrategyFactory<SelectColumnSelectorStrategy>
  {
    @Override
    public SelectColumnSelectorStrategy makeColumnSelectorStrategy(
        ColumnCapabilities capabilities,
        ColumnValueSelector selector
    )
    {
      ValueType type = capabilities.getType();
      switch (type) {
        case STRING:
          return new StringSelectColumnSelectorStrategy();
        case LONG:
          return new LongSelectColumnSelectorStrategy();
        case FLOAT:
          return new FloatSelectColumnSelectorStrategy();
        case DOUBLE:
          return new DoubleSelectColumnSelectorStrategy();
        default:
          throw new IAE("Cannot create query type helper from invalid type [%s]", type);
      }
    }
  }

  public interface SelectColumnSelectorStrategy<ValueSelectorType> extends ColumnSelectorStrategy
  {
    /**
     * Read the current row from selector and add the row values for a dimension to the result map.
     *
     * Multi-valued rows should be added to the result as a List, single value rows should be added as a single object.
     *
     * @param outputName Output name for this dimension in the select query being served
     * @param selector Dimension value selector
     * @param resultMap Row value map for the current row being retrieved by the select query
     */
    void addRowValuesToSelectResult(
        String outputName,
        ValueSelectorType selector,
        Map<String, Object> resultMap
    );
  }

  public static class StringSelectColumnSelectorStrategy implements SelectColumnSelectorStrategy<DimensionSelector>
  {
    @Override
    public void addRowValuesToSelectResult(String outputName, DimensionSelector selector, Map<String, Object> resultMap)
    {
      final IndexedInts row = selector.getRow();
      int rowSize = row.size();
      if (rowSize == 0) {
        resultMap.put(outputName, null);
      } else if (rowSize == 1) {
        final String dimVal = selector.lookupName(row.get(0));
        resultMap.put(outputName, dimVal);
      } else {
        List<String> dimVals = new ArrayList<>(rowSize);
        for (int i = 0; i < rowSize; ++i) {
          dimVals.add(selector.lookupName(row.get(i)));
        }
        resultMap.put(outputName, dimVals);
      }
    }
  }

  public static class LongSelectColumnSelectorStrategy
      implements SelectColumnSelectorStrategy<BaseLongColumnValueSelector>
  {

    @Override
    public void addRowValuesToSelectResult(
        String outputName,
        BaseLongColumnValueSelector selector,
        Map<String, Object> resultMap
    )
    {
      if (selector == null) {
        resultMap.put(outputName, null);
      } else {
        resultMap.put(outputName, selector.getLong());
      }
    }
  }

  public static class FloatSelectColumnSelectorStrategy
      implements SelectColumnSelectorStrategy<BaseFloatColumnValueSelector>
  {
    @Override
    public void addRowValuesToSelectResult(
        String outputName,
        BaseFloatColumnValueSelector selector,
        Map<String, Object> resultMap
    )
    {
      if (selector == null) {
        resultMap.put(outputName, null);
      } else {
        resultMap.put(outputName, selector.getFloat());
      }
    }
  }
  public static class DoubleSelectColumnSelectorStrategy
      implements SelectColumnSelectorStrategy<BaseDoubleColumnValueSelector>
  {
    @Override
    public void addRowValuesToSelectResult(
        String outputName,
        BaseDoubleColumnValueSelector selector,
        Map<String, Object> resultMap
    )
    {
      if (selector == null) {
        resultMap.put(outputName, null);
      } else {
        resultMap.put(outputName, selector.getDouble());
      }
    }
  }

  public Sequence<Result<SelectResultValue>> process(final SelectQuery query, final Segment segment)
  {
    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    Preconditions.checkArgument(
        query.getDataSource().getNames().size() == 1,
        "At the point where this code is called, only one data source should exist. Data sources: %s",
        query.getDataSource().getNames()
    );

    final Iterable<DimensionSpec> dims;
    if (query.getDimensions() == null || query.getDimensions().isEmpty()) {
      dims = DefaultDimensionSpec.toSpec(adapter.getAvailableDimensions());
    } else {
      dims = query.getDimensions();
    }

    final Iterable<String> metrics;
    if (query.getMetrics() == null || query.getMetrics().isEmpty()) {
      metrics = adapter.getAvailableMetrics();
    } else {
      metrics = query.getMetrics();
    }
    List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    // should be rewritten with given interval
    final String segmentId = segment.getId().withInterval(intervals.get(0)).toString();

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getDimensionsFilter()));

    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        query.getQuerySegmentSpec().getIntervals(),
        filter,
        query.getVirtualColumns(),
        query.isDescending(),
        query.getGranularity(),
        new Function<Cursor, Result<SelectResultValue>>()
        {
          @Override
          public Result<SelectResultValue> apply(Cursor cursor)
          {
            final SelectResultValueBuilder builder = new SelectResultValueBuilder(
                cursor.getTime(),
                query.getPagingSpec(),
                query.isDescending()
            );

            final BaseLongColumnValueSelector timestampColumnSelector =
                cursor.getColumnSelectorFactory().makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

            final List<ColumnSelectorPlus<SelectColumnSelectorStrategy>> selectorPlusList = Arrays.asList(
                DimensionHandlerUtils.createColumnSelectorPluses(
                    STRATEGY_FACTORY,
                    Lists.newArrayList(dims),
                    cursor.getColumnSelectorFactory()
                )
            );

            for (DimensionSpec dimSpec : dims) {
              builder.addDimension(dimSpec.getOutputName());
            }

            final Map<String, BaseObjectColumnValueSelector<?>> metSelectors = new HashMap<>();
            for (String metric : metrics) {
              final BaseObjectColumnValueSelector<?> metricSelector =
                  cursor.getColumnSelectorFactory().makeColumnValueSelector(metric);
              metSelectors.put(metric, metricSelector);
              builder.addMetric(metric);
            }

            final PagingOffset offset = query.getPagingOffset(segmentId);

            cursor.advanceTo(offset.startDelta());

            int lastOffset = offset.startOffset();
            for (; !cursor.isDone() && offset.hasNext(); cursor.advance(), offset.next()) {
              final Map<String, Object> theEvent = singleEvent(
                  EventHolder.TIMESTAMP_KEY,
                  timestampColumnSelector,
                  selectorPlusList,
                  metSelectors
              );

              builder.addEntry(
                  new EventHolder(
                      segmentId,
                      lastOffset = offset.current(),
                      theEvent
                  )
              );
            }

            builder.finished(segmentId, lastOffset);

            return builder.build();
          }
        }
    );
  }

  public static Map<String, Object> singleEvent(
      String timestampKey,
      BaseLongColumnValueSelector timestampColumnSelector,
      List<ColumnSelectorPlus<SelectColumnSelectorStrategy>> selectorPlusList,
      Map<String, BaseObjectColumnValueSelector<?>> metSelectors
  )
  {
    final Map<String, Object> theEvent = Maps.newLinkedHashMap();
    theEvent.put(timestampKey, DateTimes.utc(timestampColumnSelector.getLong()));

    for (ColumnSelectorPlus<SelectColumnSelectorStrategy> selectorPlus : selectorPlusList) {
      selectorPlus
          .getColumnSelectorStrategy()
          .addRowValuesToSelectResult(selectorPlus.getOutputName(), selectorPlus.getSelector(), theEvent);
    }

    for (Map.Entry<String, BaseObjectColumnValueSelector<?>> metSelector : metSelectors.entrySet()) {
      final String metric = metSelector.getKey();
      final BaseObjectColumnValueSelector<?> selector = metSelector.getValue();

      if (selector == null) {
        theEvent.put(metric, null);
      } else {
        theEvent.put(metric, selector.getObject());
      }
    }
    return theEvent;
  }
}
