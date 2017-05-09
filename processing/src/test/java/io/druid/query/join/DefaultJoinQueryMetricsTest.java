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

package io.druid.query.join;

import com.google.common.collect.ImmutableList;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.CachingEmitter;
import io.druid.query.DataSourceWithSegmentSpec;
import io.druid.query.DefaultQueryMetricsTest;
import io.druid.query.DruidMetrics;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.VirtualColumns;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class DefaultJoinQueryMetricsTest
{

  @Test
  public void testDefaultJoinQueryMetricsQuery()
  {
    CachingEmitter cachingEmitter = new CachingEmitter();
    ServiceEmitter serviceEmitter = new ServiceEmitter("", "", cachingEmitter);
    DefaultJoinQueryMetrics queryMetrics = new DefaultJoinQueryMetrics(new DefaultObjectMapper());
    final JoinSpec leftChildSpec = new JoinSpec(
        JoinType.INNER,
        new AndPredicate(
            ImmutableList.of(
                new EqualPredicate(
                    new DimensionPredicate(new DefaultDimensionSpec("src1", "dim1", "dim1")),
                    new DimensionPredicate(new DefaultDimensionSpec("src2", "dim1", "dim1"))
                ),
                new EqualPredicate(
                    new AddPredicate(
                        new DimensionPredicate(new DefaultDimensionSpec("src2", "dim2", "dim2")),
                        new LiteralPredicate("10")
                    ),
                    new AddPredicate(
                        new DimensionPredicate(new DefaultDimensionSpec("src1", "dim2", "dim2")),
                        new DimensionPredicate(new DefaultDimensionSpec("src1", "dim3", "dim3"))
                    )
                )
            )
        ),
        new DataSourceJoinInputSpec(new TableDataSource("src1"), QueryRunnerTestHelper.firstToThird),
        new DataSourceJoinInputSpec(new TableDataSource("src2"), QueryRunnerTestHelper.firstToThird)
    );

    final JoinSpec joinSpec = new JoinSpec(
        JoinType.INNER,
        new EqualPredicate(
            new DimensionPredicate(new DefaultDimensionSpec("j1", "dim4", "dim4")),
            new DimensionPredicate(new DefaultDimensionSpec("src3", "dim4", "dim4"))
        ),
        leftChildSpec,
        new DataSourceJoinInputSpec(new TableDataSource("src3"), QueryRunnerTestHelper.firstToThird)
    );

    JoinQuery query = JoinQuery.builder()
                               .setJoinSpec(joinSpec)
                               .setGranularity(QueryRunnerTestHelper.dayGran)
                               .setDimensions(
                                   ImmutableList.of(
                                       new DefaultDimensionSpec("src1", "dim5", "dim5"),
                                       new DefaultDimensionSpec("src2", "dim5", "dim5"),
                                       new DefaultDimensionSpec("src3", "dim5", "dim5")
                                   )
                               )
                               .setMetrics(
                                   ImmutableList.of(
                                       "met1", "met2", "met3"
                                   )
                               )
                               .setVirtualColumns(VirtualColumns.EMPTY)
                               .build();
    final DataSourceWithSegmentSpec distributionTarget = query.getDataSources().get(0);
    query = (JoinQuery) query.distributeBy(distributionTarget);
    queryMetrics.query(query);

    queryMetrics.reportQueryTime(0).emit(serviceEmitter);
    Map<String, Object> actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals(12, actualEvent.size());
    Assert.assertTrue(actualEvent.containsKey("feed"));
    Assert.assertTrue(actualEvent.containsKey("timestamp"));
    Assert.assertEquals("", actualEvent.get("host"));
    Assert.assertEquals("", actualEvent.get("service"));
    Assert.assertEquals(query.getType(), actualEvent.get(DruidMetrics.TYPE));
    Assert.assertEquals("false", actualEvent.get("hasFilters"));
    Assert.assertEquals("", actualEvent.get(DruidMetrics.ID));

    // Join-specific dimensions
    Assert.assertEquals("3", actualEvent.get("numDataSources"));
    Assert.assertEquals(
        distributionTarget.getDataSource().toString(),
        actualEvent.get("distributionTarget")
    );
    Assert.assertEquals(
        query.getDuration(distributionTarget).toString(),
        actualEvent.get("distributionTargetDuration")
    );

    // Metric
    Assert.assertEquals("query/time", actualEvent.get("metric"));
    Assert.assertEquals(0L, actualEvent.get("value"));
  }

  @Test
  public void testDefaultJoinQueryMetricsMetricNamesAndUnits()
  {
    CachingEmitter cachingEmitter = new CachingEmitter();
    ServiceEmitter serviceEmitter = new ServiceEmitter("", "", cachingEmitter);
    DefaultJoinQueryMetrics queryMetrics = new DefaultJoinQueryMetrics(new DefaultObjectMapper());
    DefaultQueryMetricsTest.testQueryMetricsDefaultMetricNamesAndUnits(cachingEmitter, serviceEmitter, queryMetrics);
  }

  @Test
  public void testDataSourcesAndDurationsAndIntervals()
  {
    CachingEmitter cachingEmitter = new CachingEmitter();
    ServiceEmitter serviceEmitter = new ServiceEmitter("", "", cachingEmitter);
    DefaultJoinQueryMetrics queryMetrics = new DefaultJoinQueryMetrics(new DefaultObjectMapper());
    final JoinSpec leftChildSpec = new JoinSpec(
        JoinType.INNER,
        new AndPredicate(
            ImmutableList.of(
                new EqualPredicate(
                    new DimensionPredicate(new DefaultDimensionSpec("src1", "dim1", "dim1")),
                    new DimensionPredicate(new DefaultDimensionSpec("src2", "dim1", "dim1"))
                ),
                new EqualPredicate(
                    new AddPredicate(
                        new DimensionPredicate(new DefaultDimensionSpec("src2", "dim2", "dim2")),
                        new LiteralPredicate("10")
                    ),
                    new AddPredicate(
                        new DimensionPredicate(new DefaultDimensionSpec("src1", "dim2", "dim2")),
                        new DimensionPredicate(new DefaultDimensionSpec("src1", "dim3", "dim3"))
                    )
                )
            )
        ),
        new DataSourceJoinInputSpec(new TableDataSource("src1"), QueryRunnerTestHelper.firstToThird),
        new DataSourceJoinInputSpec(new TableDataSource("src2"), QueryRunnerTestHelper.firstToThird)
    );

    final JoinSpec joinSpec = new JoinSpec(
        JoinType.INNER,
        new EqualPredicate(
            new DimensionPredicate(new DefaultDimensionSpec("j1", "dim4", "dim4")),
            new DimensionPredicate(new DefaultDimensionSpec("src3", "dim4", "dim4"))
        ),
        leftChildSpec,
        new DataSourceJoinInputSpec(new TableDataSource("src3"), QueryRunnerTestHelper.firstToThird)
    );

    JoinQuery query = JoinQuery.builder().setJoinSpec(joinSpec)
                               .setGranularity(QueryRunnerTestHelper.dayGran)
                               .setDimensions(
                                   ImmutableList.of(
                                       new DefaultDimensionSpec("src1", "dim5", "dim5"),
                                       new DefaultDimensionSpec("src2", "dim5", "dim5"),
                                       new DefaultDimensionSpec("src3", "dim5", "dim5")
                                   )
                               )
                               .setMetrics(
                                   ImmutableList.of(
                                       "met1", "met2", "met3"
                                   )
                               )
                               .setVirtualColumns(VirtualColumns.EMPTY)
                               .build();
    final DataSourceWithSegmentSpec distributionTarget = query.getDataSources().get(0);
    query = (JoinQuery) query.distributeBy(distributionTarget);
    queryMetrics.dataSourcesAndDurations(query);
    queryMetrics.intervals(query);

    queryMetrics.reportQueryTime(0).emit(serviceEmitter);
    Map<String, Object> actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals(8, actualEvent.size());
    Assert.assertEquals(
        ImmutableList.of(
            "{dataSource=src1,duration=PT172800S}",
            "{dataSource=src2,duration=PT172800S}",
            "{dataSource=src3,duration=PT172800S}"
        ),
        actualEvent.get("dataSourcesAndDurations")
    );
    Assert.assertEquals(
        ImmutableList.of(
            "[2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z]",
            "[2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z]",
            "[2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z]"
        ),
        actualEvent.get(DruidMetrics.INTERVAL)
    );
  }
}
