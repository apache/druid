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
package org.apache.druid.query.lookbackquery;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import org.apache.druid.query.rollingavgquery.DigitsRollingAverageQueryModule;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.Query;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LookbackQueryTest
{

  private static ObjectMapper jsonMapper;

  @BeforeClass
  public static void setUpClass()
  {

    List<com.google.inject.Module> modules = new ArrayList<>();

    modules.add(new QueryRunnerFactoryModule());
    modules.add(new QueryableModule());
    modules.add(new DruidProcessingModule());
    modules.add(
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("queryTest");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(1);
          binder.bind(QuerySegmentWalker.class).toProvider(Providers.<QuerySegmentWalker>of(null));
        }
    );

    Injector baseInjector = GuiceInjectors.makeStartupInjector();
    Injector injector = Initialization.makeInjectorWithModules(baseInjector, modules);
    jsonMapper = injector.getInstance(ObjectMapper.class);

    DruidModule druidModule = new DigitsLookbackQueryModule();
    for (Module m : druidModule.getJacksonModules()) {
      jsonMapper.registerModule(m);
    }

    DruidModule rollingAverageModule = new DigitsRollingAverageQueryModule();
    for (Module m : rollingAverageModule.getJacksonModules()) {
      jsonMapper.registerModule(m);
    }

  }

  //@Test
  public void testQuerySerializationTimeseries() throws IOException
  {
    Query query = new LookbackQuery(
        LookbackQueryTestResources.timeSeriesQueryDataSource,
        LookbackQueryTestResources.singleLookbackPostAggList,
        null,
        Collections.singletonList(LookbackQueryTestResources.period),
        null,
        null,
        null
    );

    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }

  //@Test
  public void testQuerySerializationGroupBy() throws IOException
  {
    Query query = new LookbackQuery(
        LookbackQueryTestResources.groupByQueryDataSource,
        LookbackQueryTestResources.singleLookbackPostAggList,
        null,
        Collections.singletonList(LookbackQueryTestResources.period),
        null,
        null,
        null
    );

    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }

  //@Test
  public void testQuerySerializationRollingAverage() throws IOException
  {
    Query query = new LookbackQuery(
        LookbackQueryTestResources.rollingAverageQueryDataSource,
        LookbackQueryTestResources.singleLookbackPostAggList,
        null,
        Collections.singletonList(LookbackQueryTestResources.period),
        null,
        null,
        null
    );

    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDataSource()
  {

    Query query = new LookbackQuery(
        new TableDataSource("slice1"),
        LookbackQueryTestResources.singleLookbackPostAggList,
        null,
        Collections.singletonList(LookbackQueryTestResources.period),
        null,
        null,
        null
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidQuerySegmentSpec()
  {
    QuerySegmentSpec spec = jsonMapper.convertValue(
        ImmutableMap.<String, Object>of(
            "type", "segments",

            "segments", ImmutableList
                .<Map<String, Object>>of(
                    ImmutableMap.<String, Object>of(
                        "itvl", "2011-07-01/2011-10-10",
                        "ver", "1",
                        "part", 0
                    ),
                    ImmutableMap.<String, Object>of(
                        "itvl", "2011-07-01/2011-10-10",
                        "ver", "1",
                        "part", 1
                    ),
                    ImmutableMap.<String, Object>of(
                        "itvl", "2011-11-01/2011-11-10",
                        "ver", "2",
                        "part", 10
                    )
                )
        ),
        QuerySegmentSpec.class
    );

    TimeseriesQuery innerQuery = new TimeseriesQuery(
        LookbackQueryTestResources.tableDataSource,
        spec,
        false,
        null,
        null,
        Granularities.ALL,
        Arrays.asList(LookbackQueryTestResources.pageViews, LookbackQueryTestResources.timeSpent),
        null,
        0,
        ImmutableMap.<String, Object>of("queryId", "2")
    );

    LookbackQuery query = LookbackQuery.builder()
                                       .setDatasource(new QueryDataSource(innerQuery))
                                       .setLookbackOffsets(Collections.singletonList(LookbackQueryTestResources.period))
                                       .build();
  }

  @Test(expected = NullPointerException.class)
  public void testQueryWithoutLookbackOffset()
  {
    LookbackQuery query = LookbackQuery.builder()
                                       .setDatasource(LookbackQueryTestResources.timeSeriesQueryDataSource)
                                       .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryWithPostAggNameCollision()
  {
    List<PostAggregator> lookbackCountPostAgg =
        Lists.<PostAggregator>newArrayList(
            new ArithmeticPostAggregator(
                "pageViews",
                "+",
                Lists.<PostAggregator>newArrayList(
                    new FieldAccessPostAggregator("pageViews", "pageViews"),
                    new FieldAccessPostAggregator("lookback_pageViews", "lookback_pageViews")
                )
            )
        );

    LookbackQuery query = LookbackQuery.builder()
                                       .setDatasource(LookbackQueryTestResources.timeSeriesQueryDataSource)
                                       .setPostAggregatorSpecs(lookbackCountPostAgg)
                                       .addLookbackOffset(LookbackQueryTestResources.period)
                                       .build();
  }
}
