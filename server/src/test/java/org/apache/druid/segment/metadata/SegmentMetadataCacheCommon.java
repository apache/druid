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

package org.apache.druid.segment.metadata;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.Map;

public abstract class SegmentMetadataCacheCommon extends InitializedNullHandlingTest
{
  public static final String DATASOURCE1 = "foo";
  public static final String DATASOURCE2 = "foo2";
  public static final String DATASOURCE3 = "foo3";
  public static final String SOME_DATASOURCE = "some_datasource";
  public static final String TIMESTAMP_COLUMN = "t";
  private static final InputRowSchema FOO_SCHEMA = new InputRowSchema(
      new TimestampSpec(TIMESTAMP_COLUMN, "iso", null),
      new DimensionsSpec(
          DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3"))
      ),
      null
  );

  public final List<InputRow> ROWS1 = ImmutableList.of(
      createRow(ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "dim1", "")),
      createRow(ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "dim1", "10.1")),
      createRow(ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "dim1", "2"))
  );

  public final List<InputRow> ROWS2 = ImmutableList.of(
      createRow(ImmutableMap.of("t", "2001-01-01", "m1", "4.0", "dim2", ImmutableList.of("a"))),
      createRow(ImmutableMap.of("t", "2001-01-02", "m1", "5.0", "dim2", ImmutableList.of("abc"))),
      createRow(ImmutableMap.of("t", "2001-01-03", "m1", "6.0"))
  );

  public QueryRunnerFactoryConglomerate conglomerate;
  public Closer resourceCloser;
  public QueryToolChestWarehouse queryToolChestWarehouse;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public QueryableIndex index1;
  public QueryableIndex index2;

  public QueryableIndex indexAuto1;
  public QueryableIndex indexAuto2;

  public DataSegment realtimeSegment1;
  public DataSegment segment1;
  public DataSegment segment2;
  public DataSegment segment3;
  public DataSegment segment4;
  public DataSegment segment5;

  public void setUpCommon()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
    queryToolChestWarehouse = new QueryToolChestWarehouse()
    {
      @Override
      public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
      {
        return conglomerate.findFactory(query).getToolchest();
      }
    };
  }

  public void setUpData() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();
    index1 = IndexBuilder.create()
                         .tmpDir(new File(tmpDir, "1"))
                         .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                         .schema(
                             new IncrementalIndexSchema.Builder()
                                 .withMetrics(
                                     new CountAggregatorFactory("cnt"),
                                     new DoubleSumAggregatorFactory("m1", "m1"),
                                     new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
                                 )
                                 .withRollup(false)
                                 .build()
                         )
                         .rows(ROWS1)
                         .buildMMappedIndex();

    index2 = IndexBuilder.create()
                         .tmpDir(new File(tmpDir, "2"))
                         .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                         .schema(
                             new IncrementalIndexSchema.Builder()
                                 .withMetrics(new LongSumAggregatorFactory("m1", "m1"))
                                 .withRollup(false)
                                 .build()
                         )
                         .rows(ROWS2)
                         .buildMMappedIndex();

    final InputRowSchema rowSchema = new InputRowSchema(
        new TimestampSpec("t", null, null),
        DimensionsSpec.builder().useSchemaDiscovery(true).build(),
        null
    );
    final List<InputRow> autoRows1 = ImmutableList.of(
        createRow(
            ImmutableMap.<String, Object>builder()
                        .put("t", "2023-01-01T00:00Z")
                        .put("numbery", 1.1f)
                        .put("numberyArrays", ImmutableList.of(1L, 2L, 3L))
                        .put("stringy", ImmutableList.of("a", "b", "c"))
                        .put("array", ImmutableList.of(1.1, 2.2, 3.3))
                        .put("nested", ImmutableMap.of("x", 1L, "y", 2L))
                        .build(),
            rowSchema
        )
    );
    final List<InputRow> autoRows2 = ImmutableList.of(
        createRow(
            ImmutableMap.<String, Object>builder()
                        .put("t", "2023-01-02T00:00Z")
                        .put("numbery", 1L)
                        .put("numberyArrays", ImmutableList.of(3.3, 2.2, 3.1))
                        .put("stringy", "a")
                        .put("array", ImmutableList.of(1L, 2L, 3L))
                        .put("nested", "hello")
                        .build(),
            rowSchema
        )
    );

    indexAuto1 = IndexBuilder.create()
                             .tmpDir(new File(tmpDir, "1"))
                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                             .schema(
                                 new IncrementalIndexSchema.Builder()
                                     .withTimestampSpec(rowSchema.getTimestampSpec())
                                     .withDimensionsSpec(rowSchema.getDimensionsSpec())
                                     .withMetrics(
                                         new CountAggregatorFactory("cnt"),
                                         new DoubleSumAggregatorFactory("m1", "m1"),
                                         new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
                                     )
                                     .withRollup(false)
                                     .build()
                             )
                             .rows(autoRows1)
                             .buildMMappedIndex();

    indexAuto2 = IndexBuilder.create()
                             .tmpDir(new File(tmpDir, "1"))
                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                             .schema(
                                 new IncrementalIndexSchema.Builder()
                                     .withTimestampSpec(
                                         new TimestampSpec("t", null, null)
                                     )
                                     .withDimensionsSpec(
                                         DimensionsSpec.builder().useSchemaDiscovery(true).build()
                                     )
                                     .withMetrics(
                                         new CountAggregatorFactory("cnt"),
                                         new DoubleSumAggregatorFactory("m1", "m1"),
                                         new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
                                     )
                                     .withRollup(false)
                                     .build()
                             )
                             .rows(autoRows2)
                             .buildMMappedIndex();

    segment1 =
        DataSegment.builder()
                   .dataSource(DATASOURCE1)
                   .interval(Intervals.of("2000/P1Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build();
    segment2 =
        DataSegment.builder()
                   .dataSource(DATASOURCE1)
                   .interval(Intervals.of("2001/P1Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build();
    segment3 =
        DataSegment.builder()
                   .dataSource(DATASOURCE2)
                   .interval(index2.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build();
    segment4 =
        DataSegment.builder()
                   .dataSource(SOME_DATASOURCE)
                   .interval(Intervals.of("2023-01-01T00Z/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build();
    segment5 =
        DataSegment.builder()
                   .dataSource(SOME_DATASOURCE)
                   .interval(Intervals.of("2023-01-02T00Z/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build();

    realtimeSegment1 = new DataSegment(
        DATASOURCE3,
        Intervals.of("2012/2013"),
        "version3",
        null,
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("met1", "met2"),
        new NumberedShardSpec(2, 3),
        null,
        1,
        100L,
        DataSegment.PruneSpecsHolder.DEFAULT
    );
  }

  public void tearDown() throws Exception
  {
    resourceCloser.close();
  }

  public InputRow createRow(final ImmutableMap<String, ?> map)
  {
    return MapInputRowParser.parse(FOO_SCHEMA, (Map<String, Object>) map);
  }

  InputRow createRow(final ImmutableMap<String, ?> map, InputRowSchema inputRowSchema)
  {
    return MapInputRowParser.parse(inputRowSchema, (Map<String, Object>) map);
  }

  public QueryLifecycleFactory getQueryLifecycleFactory(QuerySegmentWalker walker)
  {
    return new QueryLifecycleFactory(
        queryToolChestWarehouse,
        walker,
        new DefaultGenericQueryMetricsFactory(),
        new NoopServiceEmitter(),
        new TestRequestLogger(),
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
    );
  }

  public DataSegment newSegment(String datasource, int partitionId)
  {
    return new DataSegment(
        datasource,
        Intervals.of("2012/2013"),
        "version1",
        null,
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("met1", "met2"),
        new NumberedShardSpec(partitionId, 0),
        null,
        1,
        100L,
        DataSegment.PruneSpecsHolder.DEFAULT
    );
  }
}
