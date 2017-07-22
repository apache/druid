/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.select;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ColumnSelectorStrategy;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.query.filter.Filter;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import io.druid.timeline.DataSegmentUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
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
        ColumnCapabilities capabilities, ColumnValueSelector selector
    )
    {
      ValueType type = capabilities.getType();
      switch(type) {
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

  public interface SelectColumnSelectorStrategy<ValueSelectorType extends ColumnValueSelector> extends ColumnSelectorStrategy
  {
    /**
     * Read the current row from dimSelector and add the row values for a dimension to the result map.
     *
     * Multi-valued rows should be added to the result as a List, single value rows should be added as a single object.
     *
     * @param outputName Output name for this dimension in the select query being served
     * @param dimSelector Dimension value selector
     * @param resultMap Row value map for the current row being retrieved by the select query
     */
    void addRowValuesToSelectResult(
        String outputName,
        ValueSelectorType dimSelector,
        Map<String, Object> resultMap
    );
  }

  public static class StringSelectColumnSelectorStrategy implements SelectColumnSelectorStrategy<DimensionSelector>
  {
    @Override
    public void addRowValuesToSelectResult(String outputName, DimensionSelector selector, Map<String, Object> resultMap)
    {
      if (selector == null) {
        resultMap.put(outputName, null);
      } else {
        final IndexedInts vals = selector.getRow();

        if (vals.size() == 1) {
          final String dimVal = selector.lookupName(vals.get(0));
          resultMap.put(outputName, dimVal);
        } else {
          List<String> dimVals = new ArrayList<>(vals.size());
          for (int i = 0; i < vals.size(); ++i) {
            dimVals.add(selector.lookupName(vals.get(i)));
          }
          resultMap.put(outputName, dimVals);
        }
      }
    }
  }

  public static class LongSelectColumnSelectorStrategy implements SelectColumnSelectorStrategy<LongColumnSelector>
  {

    @Override
    public void addRowValuesToSelectResult(
        String outputName, LongColumnSelector dimSelector, Map<String, Object> resultMap
    )
    {
      if (dimSelector == null) {
        resultMap.put(outputName, null);
      } else {
        resultMap.put(outputName, dimSelector.get());
      }
    }
  }

  public static class FloatSelectColumnSelectorStrategy implements SelectColumnSelectorStrategy<FloatColumnSelector>
  {
    @Override
    public void addRowValuesToSelectResult(
        String outputName, FloatColumnSelector dimSelector, Map<String, Object> resultMap
    )
    {
      if (dimSelector == null) {
        resultMap.put(outputName, null);
      } else {
        resultMap.put(outputName, dimSelector.get());
      }
    }
  }
  public static class DoubleSelectColumnSelectorStrategy implements SelectColumnSelectorStrategy<DoubleColumnSelector>
  {
    @Override
    public void addRowValuesToSelectResult(
        String outputName,
        DoubleColumnSelector dimSelector,
        Map<String, Object> resultMap
    )
    {
      if (dimSelector == null) {
        resultMap.put(outputName, null);
      } else {
        resultMap.put(outputName, dimSelector.get());
      }
    }
  }

  private final Supplier<SelectQueryConfig> configSupplier;

  @Inject
  public SelectQueryEngine(
      Supplier<SelectQueryConfig> configSupplier
  )
  {
    this.configSupplier = configSupplier;
  }

  public Sequence<Result<SelectResultValue>> process(final SelectQuery query, final Segment segment)
  {
    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    // at the point where this code is called, only one datasource should exist.
    String dataSource = Iterables.getOnlyElement(query.getDataSource().getNames());

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
    final String segmentId = DataSegmentUtils.withInterval(dataSource, segment.getIdentifier(), intervals.get(0));

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

            final LongColumnSelector timestampColumnSelector = cursor.makeLongColumnSelector(Column.TIME_COLUMN_NAME);

            final List<ColumnSelectorPlus<SelectColumnSelectorStrategy>> selectorPlusList = Arrays.asList(
                DimensionHandlerUtils.createColumnSelectorPluses(
                    STRATEGY_FACTORY,
                    Lists.newArrayList(dims),
                    cursor
                )
            );

            for (DimensionSpec dimSpec : dims) {
              builder.addDimension(dimSpec.getOutputName());
            }

            final Map<String, ObjectColumnSelector> metSelectors = Maps.newHashMap();
            for (String metric : metrics) {
              final ObjectColumnSelector metricSelector = cursor.makeObjectColumnSelector(metric);
              metSelectors.put(metric, metricSelector);
              builder.addMetric(metric);
            }

            final PagingOffset offset = query.getPagingOffset(segmentId);

            cursor.advanceTo(offset.startDelta());

            int lastOffset = offset.startOffset();
            for (; !cursor.isDone() && offset.hasNext(); cursor.advance(), offset.next()) {
              final Map<String, Object> theEvent = singleEvent(
                  EventHolder.timestampKey,
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
      LongColumnSelector timestampColumnSelector,
      List<ColumnSelectorPlus<SelectColumnSelectorStrategy>> selectorPlusList,
      Map<String, ObjectColumnSelector> metSelectors
  )
  {
    final Map<String, Object> theEvent = Maps.newLinkedHashMap();
    theEvent.put(timestampKey, new DateTime(timestampColumnSelector.get()));

    for (ColumnSelectorPlus<SelectColumnSelectorStrategy> selectorPlus : selectorPlusList) {
      selectorPlus.getColumnSelectorStrategy().addRowValuesToSelectResult(selectorPlus.getOutputName(), selectorPlus.getSelector(), theEvent);
    }

    for (Map.Entry<String, ObjectColumnSelector> metSelector : metSelectors.entrySet()) {
      final String metric = metSelector.getKey();
      final ObjectColumnSelector selector = metSelector.getValue();

      if (selector == null) {
        theEvent.put(metric, null);
      } else {
        theEvent.put(metric, selector.get());
      }
    }
    return theEvent;
  }
}
