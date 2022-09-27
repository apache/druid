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

package org.apache.druid.compressedbigdecimal;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CompressedBigDecimalSqlAggregatorTest extends BaseCalciteQueryTest
{
  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(CalciteTests.TIMESTAMP_COLUMN, "iso", null),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3", "m2"))
          )
      )
  );

  private static final List<InputRow> ROWS1 =
      CalciteTests.RAW_ROWS1.stream().map(m -> CalciteTests.createRow(m, PARSER)).collect(Collectors.toList());

  @Override
  public Iterable<? extends Module> getJacksonModules()
  {
    CompressedBigDecimalModule bigDecimalModule = new CompressedBigDecimalModule();

    return Iterables.concat(super.getJacksonModules(), bigDecimalModule.getJacksonModules());
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker() throws IOException
  {
    CompressedBigDecimalModule bigDecimalModule = new CompressedBigDecimalModule();

    for (Module mod : bigDecimalModule.getJacksonModules()) {
      CalciteTests.getJsonMapper().registerModule(mod);
      TestHelper.JSON_MAPPER.registerModule(mod);
    }
    QueryableIndex index =
        IndexBuilder.create()
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withMetrics(
                                new CountAggregatorFactory("cnt"),
                                new DoubleSumAggregatorFactory("m1", "m1")
                            )
                            .withRollup(false)
                            .build()
                    )
                    .rows(ROWS1)
                    .buildMMappedIndex();

    walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(CalciteTests.DATASOURCE1)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index
    );
    return walker;
  }

  @Override
  public DruidOperatorTable createOperatorTable()
  {
    return new DruidOperatorTable(ImmutableSet.of(new CompressedBigDecimalSqlAggregator()), ImmutableSet.of());
  }

  @Override
  public ObjectMapper createQueryJsonMapper()
  {
    ObjectMapper objectMapper = super.createQueryJsonMapper();
    objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    return objectMapper;
  }

  @Test
  public void testCompressedBigDecimalAggWithNumberParse1()
  {
    cannotVectorize();
    testQuery(
        "SELECT big_sum(m1, 9, 9), big_sum(m2, 9, 9), big_sum(dim1, 9, 9, false) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new CompressedBigDecimalAggregatorFactory("a0:agg", "m1", 9, 9, false),
                      new CompressedBigDecimalAggregatorFactory("a1:agg", "m2", 9, 9, false),
                      new CompressedBigDecimalAggregatorFactory("a2:agg", "dim1", 9, 9, false)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{
            "21.000000000",
            "21.000000000",
            "13.100000000",
            })
    );
  }

  @Test(expected = NumberFormatException.class)
  public void testCompressedBigDecimalAggWithNumberParse2()
  {
    cannotVectorize();
    testQuery(
        "SELECT big_sum(dim1, 9, 9, true) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new CompressedBigDecimalAggregatorFactory("a0:agg", "dim1", 9, 9, true)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{"13.100000000"})
    );
  }

  @Test
  public void testCompressedBigDecimalAggDefaultNumberParse()
  {
    cannotVectorize();
    testQuery(
        "SELECT big_sum(m1, 9, 9), big_sum(m2, 9, 9), big_sum(dim1, 9, 9) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new CompressedBigDecimalAggregatorFactory("a0:agg", "m1", 9, 9, false),
                      new CompressedBigDecimalAggregatorFactory("a1:agg", "m2", 9, 9, false),
                      new CompressedBigDecimalAggregatorFactory("a2:agg", "dim1", 9, 9, false)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{
            "21.000000000",
            "21.000000000",
            "13.100000000",
            })
    );
  }

  @Test
  public void testCompressedBigDecimalAggDefaultScale()
  {
    cannotVectorize();
    testQuery(
        "SELECT big_sum(m1, 9), big_sum(m2, 9), big_sum(dim1, 9) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new CompressedBigDecimalAggregatorFactory("a0:agg", "m1", 9, 9, false),
                      new CompressedBigDecimalAggregatorFactory("a1:agg", "m2", 9, 9, false),
                      new CompressedBigDecimalAggregatorFactory("a2:agg", "dim1", 9, 9, false)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{
            "21.000000000",
            "21.000000000",
            "13.100000000"
        })
    );
  }

  @Test
  public void testCompressedBigDecimalAggDefaultSizeAndScale()
  {
    cannotVectorize();
    testQuery(
        "SELECT big_sum(m1), big_sum(m2), big_sum(dim1) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new CompressedBigDecimalAggregatorFactory("a0:agg", "m1", 6, 9, false),
                      new CompressedBigDecimalAggregatorFactory("a1:agg", "m2", 6, 9, false),
                      new CompressedBigDecimalAggregatorFactory("a2:agg", "dim1", 6, 9, false)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{
            "21.000000000",
            "21.000000000",
            "13.100000000"
        })
    );
  }

}
