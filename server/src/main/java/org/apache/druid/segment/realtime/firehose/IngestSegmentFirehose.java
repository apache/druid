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

package org.apache.druid.segment.realtime.firehose;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.transform.Transformer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
            adapters,
            new Function<WindowedStorageAdapter, Sequence<InputRow>>()
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
                                cursor.getColumnSelectorFactory().makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

                            final Map<String, DimensionSelector> dimSelectors = new HashMap<>();
                            for (String dim : dims) {
                              final DimensionSelector dimSelector = cursor
                                  .getColumnSelectorFactory()
                                  .makeDimensionSelector(new DefaultDimensionSpec(dim, dim));
                              // dimSelector is null if the dimension is not present
                              if (dimSelector != null) {
                                dimSelectors.put(dim, dimSelector);
                              }
                            }

                            final Map<String, BaseObjectColumnValueSelector> metSelectors = new HashMap<>();
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
                                        theEvent.put(TimestampSpec.DEFAULT_COLUMN, DateTimes.utc(timestamp));

                                        for (Map.Entry<String, DimensionSelector> dimSelector :
                                            dimSelectors.entrySet()) {
                                          final String dim = dimSelector.getKey();
                                          final DimensionSelector selector = dimSelector.getValue();
                                          final IndexedInts vals = selector.getRow();

                                          int valsSize = vals.size();
                                          if (valsSize == 1) {
                                            final String dimVal = selector.lookupName(vals.get(0));
                                            theEvent.put(dim, dimVal);
                                          } else if (valsSize > 1) {
                                            List<String> dimVals = new ArrayList<>(valsSize);
                                            for (int i = 0; i < valsSize; ++i) {
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
  public void close() throws IOException
  {
    rowYielder.close();
  }

}
