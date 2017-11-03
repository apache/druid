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

package io.druid.segment.realtime.firehose;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.Yielders;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.select.EventHolder;
import io.druid.segment.BaseLongColumnValueSelector;
import io.druid.segment.BaseObjectColumnValueSelector;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import io.druid.segment.transform.TransformSpec;
import io.druid.segment.transform.Transformer;
import io.druid.utils.Runnables;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IngestSegmentFirehose implements Firehose
{
  private final Transformer transformer;
  private Yielder<InputRow> rowYielder;

  public IngestSegmentFirehose(
      final List<WindowedStorageAdapter> adapters,
      final TransformSpec transformSpec,
      final List<String> dims,
      final List<String> metrics,
      final DimFilter dimFilter
  )
  {
    this.transformer = transformSpec.toTransformer();

    Sequence<InputRow> rows = Sequences.concat(
        Iterables.transform(
            adapters, new Function<WindowedStorageAdapter, Sequence<InputRow>>()
            {
              @Nullable
              @Override
              public Sequence<InputRow> apply(WindowedStorageAdapter adapter)
              {
                return Sequences.concat(
                    Sequences.map(
                        adapter.getAdapter().makeCursors(
                            Filters.toFilter(dimFilter),
                            adapter.getInterval(),
                            VirtualColumns.EMPTY,
                            Granularities.ALL,
                            false,
                            null
                        ), new Function<Cursor, Sequence<InputRow>>()
                        {
                          @Nullable
                          @Override
                          public Sequence<InputRow> apply(final Cursor cursor)
                          {
                            final BaseLongColumnValueSelector timestampColumnSelector =
                                cursor.getColumnSelectorFactory().makeColumnValueSelector(Column.TIME_COLUMN_NAME);

                            final Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
                            for (String dim : dims) {
                              final DimensionSelector dimSelector = cursor
                                  .getColumnSelectorFactory()
                                  .makeDimensionSelector(new DefaultDimensionSpec(dim, dim));
                              // dimSelector is null if the dimension is not present
                              if (dimSelector != null) {
                                dimSelectors.put(dim, dimSelector);
                              }
                            }

                            final Map<String, BaseObjectColumnValueSelector> metSelectors = Maps.newHashMap();
                            for (String metric : metrics) {
                              final BaseObjectColumnValueSelector metricSelector =
                                  cursor.getColumnSelectorFactory().makeColumnValueSelector(metric);
                              metSelectors.put(metric, metricSelector);
                            }

                            return Sequences.simple(
                                new Iterable<InputRow>()
                                {
                                  @Override
                                  public Iterator<InputRow> iterator()
                                  {
                                    return new Iterator<InputRow>()
                                    {
                                      @Override
                                      public boolean hasNext()
                                      {
                                        return !cursor.isDone();
                                      }

                                      @Override
                                      public InputRow next()
                                      {
                                        final Map<String, Object> theEvent = Maps.newLinkedHashMap();
                                        final long timestamp = timestampColumnSelector.getLong();
                                        theEvent.put(EventHolder.timestampKey, DateTimes.utc(timestamp));

                                        for (Map.Entry<String, DimensionSelector> dimSelector :
                                            dimSelectors.entrySet()) {
                                          final String dim = dimSelector.getKey();
                                          final DimensionSelector selector = dimSelector.getValue();
                                          final IndexedInts vals = selector.getRow();

                                          if (vals.size() == 1) {
                                            final String dimVal = selector.lookupName(vals.get(0));
                                            theEvent.put(dim, dimVal);
                                          } else {
                                            List<String> dimVals = Lists.newArrayList();
                                            for (int i = 0; i < vals.size(); ++i) {
                                              dimVals.add(selector.lookupName(vals.get(i)));
                                            }
                                            theEvent.put(dim, dimVals);
                                          }
                                        }

                                        for (Map.Entry<String, BaseObjectColumnValueSelector> metSelector :
                                            metSelectors.entrySet()) {
                                          final String metric = metSelector.getKey();
                                          final BaseObjectColumnValueSelector selector = metSelector.getValue();
                                          Object value = selector.getObject();
                                          if (value != null) {
                                            theEvent.put(metric, value);
                                          }
                                        }
                                        cursor.advance();
                                        return new MapBasedInputRow(timestamp, dims, theEvent);
                                      }

                                      @Override
                                      public void remove()
                                      {
                                        throw new UnsupportedOperationException("Remove Not Supported");
                                      }
                                    };
                                  }
                                }
                            );
                          }
                        }
                    )
                );
              }
            }
        )
    );
    rowYielder = Yielders.each(rows);
  }

  @Override
  public boolean hasMore()
  {
    return !rowYielder.isDone();
  }

  @Nullable
  @Override
  public InputRow nextRow()
  {
    final InputRow inputRow = rowYielder.get();
    rowYielder = rowYielder.next(null);
    return transformer.transform(inputRow);
  }

  @Override
  public Runnable commit()
  {
    return Runnables.getNoopRunnable();
  }

  @Override
  public void close() throws IOException
  {
    rowYielder.close();
  }

}
