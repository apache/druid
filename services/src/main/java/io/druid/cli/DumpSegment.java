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

package io.druid.cli;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.granularity.QueryGranularities;
import io.druid.guice.annotations.Json;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.IndexIO;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

@Command(
    name = "dump-segment",
    description = "Dump segment data"
)
public class DumpSegment extends GuiceRunnable
{
  private static final Logger log = new Logger(DumpSegment.class);

  public DumpSegment()
  {
    super(log);
  }

  @Option(
      name = {"-d", "--directory"},
      title = "directory",
      description = "Directory containing segment data",
      required = true)
  public String directory;

  @Option(
      name = {"-o", "--out"},
      title = "file",
      description = "File to write to, or omit to write to stdout",
      required = false)
  public String outputFileName;

  @Option(
      name = {"--filter"},
      title = "json",
      description = "Filter, JSON encoded, or omit to include all rows",
      required = false)
  public String filterJson = null;

  @Option(
      name = {"-c", "--column"},
      title = "column",
      description = "Column to include, specify multiple times for multiple columns, or omit to include all columns",
      required = false)
  public List<String> columnNames = Lists.newArrayList();

  @Option(
      name = "--time-iso8601",
      title = "Dump __time column in ISO8601 format rather than long",
      required = false)
  public boolean timeISO8601 = false;

  @Option(
      name = "--metadata",
      title = "Dump metadata instead of actual rows, will ignore --filter and --column selections",
      required = false)
  public boolean metadata = false;

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    final IndexIO indexIO = injector.getInstance(IndexIO.class);

    try (final QueryableIndex index = indexIO.loadIndex(new File(directory))) {
      if (metadata) {
        runMetadata(injector, index);
      } else {
        runDump(injector, index);
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void runMetadata(final Injector injector, final QueryableIndex index) throws IOException
  {
    final ObjectMapper objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final SegmentMetadataQuery query = new SegmentMetadataQuery(
        new TableDataSource("dataSource"),
        new SpecificSegmentSpec(new SegmentDescriptor(index.getDataInterval(), "0", 0)),
        null,
        false,
        null,
        EnumSet.allOf(SegmentMetadataQuery.AnalysisType.class),
        false,
        false
    );
    withOutputStream(
        new Function<OutputStream, Object>()
        {
          @Override
          public Object apply(final OutputStream out)
          {
            evaluateSequenceForSideEffects(
                Sequences.map(
                    executeQuery(injector, index, query),
                    new Function<SegmentAnalysis, Object>()
                    {
                      @Override
                      public Object apply(SegmentAnalysis analysis)
                      {
                        try {
                          objectMapper.copy()
                                      .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
                                      .writeValue(out, analysis);
                        }
                        catch (IOException e) {
                          throw Throwables.propagate(e);
                        }
                        return null;
                      }
                    }
                )
            );

            return null;
          }
        }
    );
  }

  private void runDump(final Injector injector, final QueryableIndex index) throws IOException
  {
    final ObjectMapper objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);

    // Empty columnNames => include all columns
    if (columnNames.isEmpty()) {
      columnNames.add(Column.TIME_COLUMN_NAME);
      Iterables.addAll(columnNames, index.getColumnNames());
    } else {
      // Remove any provided columnNames that do not exist in this segment
      for (String columnName : ImmutableList.copyOf(columnNames)) {
        if (index.getColumn(columnName) == null) {
          columnNames.remove(columnName);
        }
      }
    }

    final DimFilter filter = filterJson != null ? objectMapper.readValue(filterJson, DimFilter.class) : null;

    final Sequence<Cursor> cursors = adapter.makeCursors(
        Filters.toFilter(filter),
        index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
        QueryGranularities.ALL,
        false
    );

    withOutputStream(
        new Function<OutputStream, Object>()
        {
          @Override
          public Object apply(final OutputStream out)
          {
            final Sequence<Object> sequence = Sequences.map(
                cursors,
                new Function<Cursor, Object>()
                {
                  @Override
                  public Object apply(Cursor cursor)
                  {
                    final List<ObjectColumnSelector> selectors = Lists.newArrayList();

                    for (String columnName : columnNames) {
                      selectors.add(makeSelector(columnName, index.getColumn(columnName), cursor));
                    }

                    while (!cursor.isDone()) {
                      final Map<String, Object> row = Maps.newLinkedHashMap();

                      for (int i = 0; i < columnNames.size(); i++) {
                        final String columnName = columnNames.get(i);
                        final Object value = selectors.get(i).get();

                        if (timeISO8601 && columnNames.get(i).equals(Column.TIME_COLUMN_NAME)) {
                          row.put(columnName, new DateTime(value, DateTimeZone.UTC).toString());
                        } else {
                          row.put(columnName, value);
                        }
                      }

                      try {
                        out.write(objectMapper.writeValueAsBytes(row));
                        out.write('\n');
                      }
                      catch (IOException e) {
                        throw Throwables.propagate(e);
                      }

                      cursor.advance();
                    }

                    return null;
                  }
                }
            );

            evaluateSequenceForSideEffects(sequence);

            return null;
          }
        }
    );
  }

  private <T> T withOutputStream(Function<OutputStream, T> f) throws IOException
  {
    if (outputFileName == null) {
      return f.apply(System.out);
    } else {
      try (final OutputStream out = new FileOutputStream(outputFileName)) {
        return f.apply(out);
      }
    }
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/tool");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(9999);
            binder.bind(DruidProcessingConfig.class).toInstance(
                new DruidProcessingConfig()
                {
                  @Override
                  public String getFormatString()
                  {
                    return "processing-%s";
                  }

                  @Override
                  public int intermediateComputeSizeBytes()
                  {
                    return 100 * 1024 * 1024;
                  }

                  @Override
                  public int getNumThreads()
                  {
                    return 1;
                  }
                }
            );
            binder.bind(ColumnConfig.class).to(DruidProcessingConfig.class);
          }
        }
    );
  }


  private static <T> Sequence<T> executeQuery(final Injector injector, final QueryableIndex index, final Query<T> query)
  {
    final QueryRunnerFactoryConglomerate conglomerate = injector.getInstance(QueryRunnerFactoryConglomerate.class);
    final QueryRunnerFactory factory = conglomerate.findFactory(query);
    final QueryRunner<T> runner = factory.createRunner(new QueryableIndexSegment("segment", index));
    final Sequence results = factory.getToolchest().mergeResults(
        factory.mergeRunners(MoreExecutors.sameThreadExecutor(), ImmutableList.<QueryRunner>of(runner))
    ).run(query, Maps.<String, Object>newHashMap());
    return (Sequence<T>) results;
  }

  private static <T> void evaluateSequenceForSideEffects(final Sequence<T> sequence)
  {
    sequence.accumulate(
        null,
        new Accumulator<Object, T>()
        {
          @Override
          public Object accumulate(Object accumulated, T in)
          {
            return null;
          }
        }
    );
  }

  private static ObjectColumnSelector makeSelector(
      final String columnName,
      final Column column,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    final ObjectColumnSelector selector;

    if (column.getDictionaryEncoding() != null) {
      // Special case for dimensions -> always wrap multi-value in arrays
      final DimensionSelector dimensionSelector = columnSelectorFactory.makeDimensionSelector(
          new DefaultDimensionSpec(columnName, columnName)
      );
      if (column.getDictionaryEncoding().hasMultipleValues()) {
        return new ObjectColumnSelector<List>()
        {
          @Override
          public Class<List> classOfObject()
          {
            return List.class;
          }

          @Override
          public List<String> get()
          {
            final IndexedInts row = dimensionSelector.getRow();
            if (row.size() == 0) {
              return null;
            } else {
              final List<String> retVal = Lists.newArrayList();
              for (int i = 0; i < row.size(); i++) {
                retVal.add(dimensionSelector.lookupName(row.get(i)));
              }
              return retVal;
            }
          }
        };
      } else {
        return new ObjectColumnSelector<String>()
        {
          @Override
          public Class<String> classOfObject()
          {
            return String.class;
          }

          @Override
          public String get()
          {
            final IndexedInts row = dimensionSelector.getRow();
            return row.size() == 0 ? null : dimensionSelector.lookupName(row.get(0));
          }
        };
      }
    } else {
      final ObjectColumnSelector maybeSelector = columnSelectorFactory.makeObjectColumnSelector(columnName);
      if (maybeSelector != null) {
        selector = maybeSelector;
      } else {
        // Selector failed to create (unrecognized column type?)
        log.warn("Could not create selector for column[%s], returning null.", columnName);
        selector = new ObjectColumnSelector()
        {
          @Override
          public Class classOfObject()
          {
            return Object.class;
          }

          @Override
          public Object get()
          {
            return null;
          }
        };
      }
    }

    return selector;
  }
}
