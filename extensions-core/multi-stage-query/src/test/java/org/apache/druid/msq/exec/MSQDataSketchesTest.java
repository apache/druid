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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Test;

/**
 * Tests of MSQ with functions from the "druid-datasketches" extension.
 */
public class MSQDataSketchesTest extends MSQTestBase
{
  @Test
  public void testHavingOnDsHll()
  {
    RowSignature resultSignature =
        RowSignature.builder()
                    .add("dim2", ColumnType.STRING)
                    .add("col", ColumnType.ofComplex("HLLSketchBuild"))
                    .build();

    GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                    .setAggregatorSpecs(
                        aggregators(
                            new HllSketchBuildAggregatorFactory("a0", "m1", 12, "HLL_4", null, false, true)
                        )
                    )
                    .setHavingSpec(having(expressionFilter(("(hll_sketch_estimate(\"a0\") > 1)"))))
                    .setContext(DEFAULT_MSQ_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("SELECT dim2, DS_HLL(m1) as col\n"
                + "FROM foo\n"
                + "GROUP BY dim2\n"
                + "HAVING HLL_SKETCH_ESTIMATE(col) > 1")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(new ColumnMappings(ImmutableList.of(
                                       new ColumnMapping("d0", "dim2"),
                                       new ColumnMapping("a0", "col")
                                   )))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(
            NullHandling.sqlCompatible()
            ? ImmutableList.of(
                new Object[]{null, "\"AgEHDAMIAgCOlN8Fp9xhBA==\""},
                new Object[]{"a", "\"AgEHDAMIAgALpZ0PPgu1BA==\""}
            )
            : ImmutableList.of(
                new Object[]{"", "\"AgEHDAMIAwCOlN8FjkSVCqfcYQQ=\""},
                new Object[]{"a", "\"AgEHDAMIAgALpZ0PPgu1BA==\""}
            )
        )
        .verifyResults();
  }

  @Test
  public void testEmptyHllSketch()
  {
    RowSignature resultSignature =
        RowSignature.builder()
                    .add("c", ColumnType.LONG)
                    .build();

    GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(
                        aggregators(
                            new FilteredAggregatorFactory(
                                new HllSketchBuildAggregatorFactory("a0", "dim2", 12, "HLL_4", null, true, true),
                                equality("dim1", "nonexistent", ColumnType.STRING),
                                "a0"
                            )
                        )
                    )
                    .setContext(DEFAULT_MSQ_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("SELECT APPROX_COUNT_DISTINCT_DS_HLL(dim2) FILTER(WHERE dim1 = 'nonexistent') AS c FROM druid.foo")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(new ColumnMappings(ImmutableList.of(
                                       new ColumnMapping("a0", "c"))
                                   ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{0L}
            )
        )
        .verifyResults();
  }
}
