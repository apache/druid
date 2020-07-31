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

package org.apache.druid.segment.join;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnProcessorFactory;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.table.RowBasedIndexedTable;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JoinTestHelper
{
  private static final Logger log = new Logger(JoinTestHelper.class);
  private static final String FACT_TIME_COLUMN = "time";
  private static final List<DimensionSchema> FACT_DIMENSIONS = ImmutableList.of(
      new StringDimensionSchema("channel"),
      new StringDimensionSchema("regionIsoCode"),
      new LongDimensionSchema("countryNumber"),
      new StringDimensionSchema("countryIsoCode"),
      new StringDimensionSchema("user"),
      new StringDimensionSchema("isRobot"),
      new StringDimensionSchema("isAnonymous"),
      new StringDimensionSchema("namespace"),
      new StringDimensionSchema("page"),
      new LongDimensionSchema("delta")
  );
  private static final RowSignature COUNTRIES_SIGNATURE =
      RowSignature.builder()
                  .add("countryNumber", ValueType.LONG)
                  .add("countryIsoCode", ValueType.STRING)
                  .add("countryName", ValueType.STRING)
                  .build();

  private static final RowSignature REGIONS_SIGNATURE =
      RowSignature.builder()
                  .add("regionIsoCode", ValueType.STRING)
                  .add("countryIsoCode", ValueType.STRING)
                  .add("regionName", ValueType.STRING)
                  .add("extraField", ValueType.STRING)
                  .build();

  private static final ColumnProcessorFactory<Supplier<Object>> SIMPLE_READER =
      new ColumnProcessorFactory<Supplier<Object>>()
      {
        @Override
        public ValueType defaultType()
        {
          return ValueType.STRING;
        }

        @Override
        public Supplier<Object> makeDimensionProcessor(DimensionSelector selector, boolean multiValue)
        {
          return selector::defaultGetObject;
        }

        @Override
        public Supplier<Object> makeFloatProcessor(BaseFloatColumnValueSelector selector)
        {
          return () -> NullHandling.sqlCompatible() && selector.isNull() ? null : selector.getFloat();
        }

        @Override
        public Supplier<Object> makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
        {
          return () -> NullHandling.sqlCompatible() && selector.isNull() ? null : selector.getDouble();
        }

        @Override
        public Supplier<Object> makeLongProcessor(BaseLongColumnValueSelector selector)
        {
          return () -> NullHandling.sqlCompatible() && selector.isNull() ? null : selector.getLong();
        }

        @Override
        public Supplier<Object> makeComplexProcessor(BaseObjectColumnValueSelector<?> selector)
        {
          return selector::getObject;
        }
      };

  public static final String INDEXED_TABLE_VERSION = DateTimes.nowUtc().toString();

  private static RowAdapter<Map<String, Object>> createMapRowAdapter(final RowSignature signature)
  {
    return new RowAdapter<Map<String, Object>>()
    {
      @Override
      public ToLongFunction<Map<String, Object>> timestampFunction()
      {
        return row -> 0L;
      }

      @Override
      public Function<Map<String, Object>, Object> columnFunction(String columnName)
      {
        final ValueType columnType = signature.getColumnType(columnName).orElse(null);

        if (columnType == null) {
          return row -> row.get(columnName);
        } else {
          return row -> DimensionHandlerUtils.convertObjectToType(row.get(columnName), columnType, false);
        }
      }
    };
  }

  public static IndexBuilder createFactIndexBuilder(final File tmpDir) throws IOException
  {
    return createFactIndexBuilder(tmpDir, -1);
  }

  public static IndexBuilder createFactIndexBuilder(final File tmpDir, final int numRows) throws IOException
  {
    return withRowsFromResource(
        "/wikipedia/data.json",
        rows -> IndexBuilder
            .create()
            .tmpDir(tmpDir)
            .schema(
                new IncrementalIndexSchema.Builder()
                    .withDimensionsSpec(new DimensionsSpec(FACT_DIMENSIONS))
                    .withMetrics(new HyperUniquesAggregatorFactory("channel_uniques", "channel"))
                    .withQueryGranularity(Granularities.NONE)
                    .withRollup(false)
                    .withMinTimestamp(DateTimes.of("2015-09-12").getMillis())
                    .build()
            )
            .rows(
                () ->
                    IntStream.rangeClosed(0, numRows < 0 ? 0 : (numRows / rows.size() + 1))
                             .boxed()
                             .flatMap(
                                 i ->
                                     rows.stream()
                                         .map(
                                             row ->
                                                 (InputRow) new MapBasedInputRow(
                                                     DateTimes.of((String) row.get(FACT_TIME_COLUMN)),
                                                     FACT_DIMENSIONS.stream()
                                                                    .map(DimensionSchema::getName)
                                                                    .collect(Collectors.toList()),
                                                     row
                                                 )
                                         )
                             )
                             .limit(numRows < 0 ? Long.MAX_VALUE : numRows)
                             .iterator()
            )
    );
  }

  public static MapLookupExtractor createCountryIsoCodeToNameLookup() throws IOException
  {
    return withRowsFromResource(
        "/wikipedia/countries.json",
        rows -> {
          final LinkedHashMap<String, String> lookupMap = new LinkedHashMap<>();

          for (Map<String, Object> row : rows) {
            lookupMap.put(
                (String) row.get("countryIsoCode"),
                (String) row.get("countryName")
            );
          }

          return new MapLookupExtractor(lookupMap, false);
        }
    );
  }

  public static MapLookupExtractor createCountryNumberToNameLookup() throws IOException
  {
    return withRowsFromResource(
        "/wikipedia/countries.json",
        rows -> new MapLookupExtractor(
            rows.stream()
                .collect(
                    Collectors.toMap(
                        row -> row.get("countryNumber").toString(),
                        row -> (String) row.get("countryName")
                    )
                ),
            false
        )
    );
  }

  public static RowBasedIndexedTable<Map<String, Object>> createCountriesIndexedTable() throws IOException
  {
    return withRowsFromResource(
        "/wikipedia/countries.json",
        rows -> new RowBasedIndexedTable<>(
            rows,
            createMapRowAdapter(COUNTRIES_SIGNATURE),
            COUNTRIES_SIGNATURE,
            ImmutableSet.of("countryNumber", "countryIsoCode"),
            INDEXED_TABLE_VERSION
        )
    );
  }

  public static RowBasedIndexedTable<Map<String, Object>> createRegionsIndexedTable() throws IOException
  {
    return withRowsFromResource(
        "/wikipedia/regions.json",
        rows -> new RowBasedIndexedTable<>(
            rows,
            createMapRowAdapter(REGIONS_SIGNATURE),
            REGIONS_SIGNATURE,
            ImmutableSet.of("regionIsoCode", "countryIsoCode"),
            INDEXED_TABLE_VERSION
        )
    );
  }

  public static List<Object[]> readCursors(final Sequence<Cursor> cursors, final List<String> columns)
  {
    return cursors.flatMap(
        cursor -> {
          final List<Supplier<Object>> readers = columns
              .stream()
              .map(
                  column ->
                      ColumnProcessors.makeProcessor(
                          column,
                          SIMPLE_READER,
                          cursor.getColumnSelectorFactory()
                      )
              )
              .collect(Collectors.toList());

          final List<Object[]> rows = new ArrayList<>();

          while (!cursor.isDone()) {
            final Object[] row = new Object[columns.size()];

            for (int i = 0; i < row.length; i++) {
              row[i] = readers.get(i).get();
            }

            rows.add(row);
            cursor.advance();
          }

          return Sequences.simple(rows);
        }
    ).toList();
  }

  public static void verifyCursors(
      final Sequence<Cursor> cursors,
      final List<String> columns,
      final List<Object[]> expectedRows
  )
  {
    final List<Object[]> rows = readCursors(cursors, columns);

    for (int i = 0; i < rows.size(); i++) {
      try {
        log.info("Row #%-2d: %s", i, TestHelper.JSON_MAPPER.writeValueAsString(rows.get(i)));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    Assert.assertEquals("number of rows", expectedRows.size(), rows.size());

    for (int i = 0; i < rows.size(); i++) {
      Assert.assertArrayEquals("row #" + i, expectedRows.get(i), rows.get(i));
    }
  }

  private static <T> T withRowsFromResource(
      final String resource,
      final Function<List<Map<String, Object>>, T> f
  ) throws IOException
  {
    final ObjectMapper jsonMapper = TestHelper.JSON_MAPPER;

    try (
        final InputStream in = JoinTestHelper.class.getResourceAsStream(resource);
        final MappingIterator<Map<String, Object>> iter = jsonMapper.readValues(
            jsonMapper.getFactory().createParser(in),
            JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
        )
    ) {
      if (in == null) {
        throw new ISE("No such resource: %s", resource);
      }
      return f.apply(Lists.newArrayList(iter));
    }
  }
}
