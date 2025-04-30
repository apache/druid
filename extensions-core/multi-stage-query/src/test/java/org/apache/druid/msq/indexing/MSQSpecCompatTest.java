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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.quidem.ProjectPathUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MSQSpecCompatTest
{
  private static final ObjectMapper JSON_MAPPER = CalciteTests.getJsonMapper();
  private static final boolean OVERWRITE = false;

  @Test
  public void testBuilder1(TestInfo info) throws Exception
  {
    RowSignature resultSignature = RowSignature.builder()
        .add("EXPR$0", ColumnType.LONG)
        .build();

    Map<String, Object> context = ImmutableMap.<String, Object>builder()
        .put("someThing", 111)
        .put("sqlInsertSegmentGranularity", "\"DAY\"")
        .build();
    MSQSpec.builder()
    .query(
        Druids.newScanQueryBuilder()
            .dataSource(
                InlineDataSource.fromIterable(
                    ImmutableList.of(new Object[]{2L}),
                    resultSignature
                )
            )
//            .setInterval(Intervals.ONLY_ETERNITY)
            .intervals(QuerySegmentSpec.ETERNITY)
            .columns("EXPR$0")
            .columnTypes(ColumnType.LONG)
            .context(context)
            .build()
    )
    .columnMappings(ColumnMappings.identity(resultSignature))
    .tuningConfig(MSQTuningConfig.defaultConfig())
    .destination(
                  DurableStorageMSQDestination.INSTANCE
                 )
    .build()
;

    MSQSpec msqSpec = new MSQSpec(
        GroupByQuery.builder()
            .setDataSource("foo")
            .setInterval(Intervals.ONLY_ETERNITY)
            .setDimFilter(new NotDimFilter(new NullFilter("dim1", null)))
            .setGranularity(Granularities.ALL)
            .setDimensions(
                new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                new DefaultDimensionSpec("dim1", "d1", ColumnType.STRING)
            )
            .setContext(
                ImmutableMap.<String, Object>builder()
                    .put("__user", "allowAll")
                    .put("finalize", true)
                    .put("maxNumTasks", 2)
                    .put("maxParseExceptions", 0)
                    .put("sqlInsertSegmentGranularity", "\"DAY\"")
                    .put("sqlQueryId", "test-query")
                    .put("sqlStringifyArrays", false)
                    .build()
            )
            .setLimitSpec(
                DefaultLimitSpec.builder()
                    .orderBy(OrderByColumnSpec.asc("d1"))
                    .build()
            )
            .setAggregatorSpecs(new CountAggregatorFactory("a0"))
            .setQuerySegmentSpec(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
            .build(),
        new ColumnMappings(
            ImmutableList.of(
                new ColumnMapping("d0", "__time"),
                new ColumnMapping("d1", "dim1"),
                new ColumnMapping("a0", "cnt")
            )
        ),
        new DataSourceMSQDestination(
            "foo1",
            Granularity.fromString("DAY"),
            null,
            null,
            null,
            null,
            null
        ),
        WorkerAssignmentStrategy.MAX,
        MSQTuningConfig.defaultConfig()
    );

    validateMSQSpecCompat(info, msqSpec, MSQSpec.class);
  }

  private void validateMSQSpecCompat(TestInfo info, MSQSpec msqSpec, Class<MSQSpec> valueType) throws Exception
  {
    String str = JSON_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(msqSpec);
    MSQSpec readBack = JSON_MAPPER.readValue(str, valueType);
    assertEquals(msqSpec, readBack);

    Path testDataPath = getTestDataPath(info);
    if (OVERWRITE) {
      Files.writeString(testDataPath, str);
    } else {
      String str2 = Files.readString(testDataPath);
      MSQSpec readBack2 = JSON_MAPPER.readValue(str2, valueType);
      assertEquals(msqSpec, readBack2);
    }
  }

  private Path getTestDataPath(TestInfo info)
  {
    String testMethodName = info.getTestMethod().get().getName();
    File f = ProjectPathUtils.getPathFromProjectRoot(
        "extensions-core/multi-stage-query/src/test/resources/" + getClass().getName() + "/" + testMethodName + ".json"
    );
    Path p = f.toPath();
    return p;
  }
}
