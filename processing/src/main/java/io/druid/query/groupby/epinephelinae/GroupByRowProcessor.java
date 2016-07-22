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

package io.druid.query.groupby.epinephelinae;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.FilteredSequence;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.collections.BlockingPool;
import io.druid.collections.ResourceHolder;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.strategy.GroupByStrategyV2;
import io.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV2.GroupByMergingKey;
import io.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV2.GroupByMergingKeySerdeFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class GroupByRowProcessor
{
  private static final Logger log = new Logger(GroupByRowProcessor.class);

  public static Sequence<Row> process(
      final Query queryParam,
      final Sequence<Row> rows,
      final GroupByQueryConfig config,
      final BlockingPool<ByteBuffer> mergeBufferPool,
      final ObjectMapper spillMapper
  )
  {
    final GroupByQuery query = (GroupByQuery) queryParam;

    final AggregatorFactory[] aggregatorFactories = new AggregatorFactory[query.getAggregatorSpecs().size()];
    for (int i = 0; i < query.getAggregatorSpecs().size(); i++) {
      aggregatorFactories[i] = query.getAggregatorSpecs().get(i);
    }

    final GroupByMergingKeySerdeFactory keySerdeFactory = new GroupByMergingKeySerdeFactory(
        query.getDimensions().size(),
        config.getMaxMergingDictionarySize()
    );
    final RowBasedColumnSelectorFactory columnSelectorFactory = new RowBasedColumnSelectorFactory();

    final File temporaryStorageDirectory = new File(
        System.getProperty("java.io.tmpdir"),
        String.format("druid-groupBy-%s_%s", UUID.randomUUID(), query.getId())
    );

    final LimitedTemporaryStorage temporaryStorage = new LimitedTemporaryStorage(
        temporaryStorageDirectory,
        config.getMaxOnDiskStorage()
    );

    final Number timeout = query.getContextValue(QueryContextKeys.TIMEOUT, null);

    final ResourceHolder<ByteBuffer> mergeBufferHolder;

    try {
      mergeBufferHolder = mergeBufferPool.take(timeout != null && timeout.longValue() > 0 ? timeout.longValue() : -1);
    }
    catch (InterruptedException e) {
      CloseQuietly.close(temporaryStorage);
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }

    try {
      return new ResourceClosingSequence<>(
          new BaseSequence<>(
              new BaseSequence.IteratorMaker<Row, CloseableGrouperIterator<GroupByMergingKey, Row>>()
              {
                @Override
                public CloseableGrouperIterator<GroupByMergingKey, Row> make()
                {
                  final Grouper<GroupByMergingKey> grouper = new SpillingGrouper<>(
                      mergeBufferHolder.get(),
                      keySerdeFactory,
                      columnSelectorFactory,
                      aggregatorFactories,
                      temporaryStorage,
                      spillMapper,
                      config.getBufferGrouperMaxSize(),
                      GroupByStrategyV2.getBufferGrouperInitialBuckets(config, query)
                  );

                  final DimensionSelector[] dimensionSelectors = new DimensionSelector[query.getDimensions().size()];
                  for (int i = 0; i < dimensionSelectors.length; i++) {
                    dimensionSelectors[i] = columnSelectorFactory.makeDimensionSelector(query.getDimensions().get(i));
                  }

                  final QueryGranularity queryGran = query.getGranularity();

                  final Accumulator<Grouper<GroupByMergingKey>, Row> accumulator = new Accumulator<Grouper<GroupByMergingKey>, Row>()
                  {
                    @Override
                    public Grouper<GroupByMergingKey> accumulate(
                        final Grouper<GroupByMergingKey> theGrouper,
                        final Row row
                    )
                    {
                      long timestamp = queryGran.truncate(row.getTimestampFromEpoch());

                      if (queryGran instanceof AllGranularity) {
                        timestamp = query.getIntervals().get(0).getStartMillis();
                      }

                      columnSelectorFactory.setRow(row);

                      final String[] dimensions = new String[query.getDimensions().size()];
                      for (int i = 0; i < dimensions.length; i++) {
                        IndexedInts index = dimensionSelectors[i].getRow();
                        String value = index.size() == 0 ? "" : dimensionSelectors[i].lookupName(index.get(0));
                        dimensions[i] = Strings.nullToEmpty(value);
                      }

                      final boolean didAggregate = theGrouper.aggregate(new GroupByMergingKey(timestamp, dimensions));
                      if (!didAggregate) {
                        throw new ISE("Grouping resources exhausted");
                      }
                      columnSelectorFactory.setRow(null);

                      return theGrouper;
                    }
                  };

                  final List<Interval> queryIntervals = query.getIntervals();
                  Filter filter = Filters.convertToCNFFromQueryContext(
                      query,
                      Filters.toFilter(query.getDimFilter())
                  );
                  final RowBasedValueMatcherFactory filterMatcherFactory = new RowBasedValueMatcherFactory();
                  final ValueMatcher filterMatcher = filter == null
                                                     ? new BooleanValueMatcher(true)
                                                     : filter.makeMatcher(filterMatcherFactory);

                  FilteredSequence<Row> filteredSequence = new FilteredSequence<>(
                      rows,
                      new Predicate<Row>()
                      {
                        @Override
                        public boolean apply(Row input)
                        {
                          boolean inInterval = false;
                          DateTime rowTime = input.getTimestamp();
                          for (Interval queryInterval : queryIntervals) {
                            if (queryInterval.contains(rowTime)) {
                              inInterval = true;
                            }
                          }
                          if (!inInterval) {
                            return false;
                          }
                          filterMatcherFactory.setRow(input);
                          return filterMatcher.matches();
                        }
                      }
                  );
                  filteredSequence.accumulate(grouper, accumulator);

                  return new CloseableGrouperIterator<>(
                      grouper,
                      true,
                      new Function<Grouper.Entry<GroupByMergingKey>, Row>()
                      {
                        @Override
                        public Row apply(Grouper.Entry<GroupByMergingKey> entry)
                        {
                          Map<String, Object> theMap = Maps.newLinkedHashMap();

                          // Add dimensions.
                          for (int i = 0; i < entry.getKey().getDimensions().length; i++) {
                            theMap.put(
                                query.getDimensions().get(i).getOutputName(),
                                Strings.emptyToNull(entry.getKey().getDimensions()[i])
                            );
                          }

                          // Add aggregations.
                          for (int i = 0; i < entry.getValues().length; i++) {
                            theMap.put(query.getAggregatorSpecs().get(i).getName(), entry.getValues()[i]);
                          }

                          return new MapBasedRow(
                              query.getGranularity().toDateTime(entry.getKey().getTimestamp()),
                              theMap
                          );
                        }
                      }
                  );
                }

                @Override
                public void cleanup(CloseableGrouperIterator<GroupByMergingKey, Row> iterFromMake)
                {
                  iterFromMake.close();
                }
              }
          ),
          new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              mergeBufferHolder.close();
              CloseQuietly.close(temporaryStorage);
            }
          }
      );
    }
    catch (Exception e) {
      // Exception caught while creating the sequence; release resources.
      mergeBufferHolder.close();
      CloseQuietly.close(temporaryStorage);
      throw e;
    }
  }

  private static class RowBasedColumnSelectorFactory implements ColumnSelectorFactory
  {
    private Row row = null;

    public void setRow(Row row)
    {
      this.row = row;
    }

    @Override
    public DimensionSelector makeDimensionSelector(
        DimensionSpec dimensionSpec
    )
    {
      return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
    }

    private DimensionSelector makeDimensionSelectorUndecorated(
        DimensionSpec dimensionSpec
    )
    {
      final String dimension = dimensionSpec.getDimension();
      final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

      return new DimensionSelector()
      {
        @Override
        public IndexedInts getRow()
        {
          final List<String> dimensionValues = row.getDimension(dimension);
          final ArrayList<Integer> vals = Lists.newArrayList();
          if (dimensionValues != null) {
            for (int i = 0; i < dimensionValues.size(); ++i) {
              vals.add(i);
            }
          }

          return new IndexedInts()
          {
            @Override
            public int size()
            {
              return vals.size();
            }

            @Override
            public int get(int index)
            {
              return vals.get(index);
            }

            @Override
            public Iterator<Integer> iterator()
            {
              return vals.iterator();
            }

            @Override
            public void close() throws IOException
            {

            }

            @Override
            public void fill(int index, int[] toFill)
            {
              throw new UnsupportedOperationException("fill not supported");
            }
          };
        }

        @Override
        public int getValueCardinality()
        {
          throw new UnsupportedOperationException("value cardinality is unknown");
        }

        @Override
        public String lookupName(int id)
        {
          final String value = row.getDimension(dimension).get(id);
          return extractionFn == null ? value : extractionFn.apply(value);
        }

        @Override
        public int lookupId(String name)
        {
          if (extractionFn != null) {
            throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
          }
          return row.getDimension(dimension).indexOf(name);
        }
      };
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(final String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          return row.getFloatMetric(columnName);
        }
      };
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(final String columnName)
    {
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          return row.getLongMetric(columnName);
        }
      };
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(final String columnName)
    {
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public Object get()
        {
          return row.getRaw(columnName);
        }
      };
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return null;
    }
  }

  private static class RowBasedValueMatcherFactory implements ValueMatcherFactory
  {
    private Row row;

    public void setRow(Row row)
    {
      this.row = row;
    }

    @Override
    public ValueMatcher makeValueMatcher(final String dimension, final Comparable value)
    {
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          return row.getDimension(dimension).contains(value);
        }
      };
    }

    @Override
    public ValueMatcher makeValueMatcher(final String dimension, final DruidPredicateFactory predicateFactory)
    {
      return new ValueMatcher()
      {
        Predicate<String> predicate = predicateFactory.makeStringPredicate();
        @Override
        public boolean matches()
        {
          List<String> values = row.getDimension(dimension);
          for (String value : values) {
            if (predicate.apply(value)) {
              return true;
            }
          }
          return false;
        }
      };
    }
  }
}
