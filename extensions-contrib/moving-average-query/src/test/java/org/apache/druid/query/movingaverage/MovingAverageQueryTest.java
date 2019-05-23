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

package org.apache.druid.query.movingaverage;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Maps;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.guava.Accumulators;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.Result;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.movingaverage.test.TestConfig;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.server.ClientQuerySegmentWalker;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.timeline.TimelineLookup;
import org.hamcrest.core.IsInstanceOf;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Base class for implementing MovingAverageQuery tests
 */
@RunWith(Parameterized.class)
public class MovingAverageQueryTest
{
  private final ObjectMapper jsonMapper;
  private final Injector injector;
  private final QueryToolChestWarehouse warehouse;
  private final RetryQueryRunnerConfig retryConfig;
  private final ServerConfig serverConfig;

  private final List<Row> groupByResults = new ArrayList<>();
  private final List<Result<TimeseriesResultValue>> timeseriesResults = new ArrayList<>();

  private final TestConfig config;
  private final String yamlFile;

  @Parameters(name = "{0}")
  public static Iterable<String[]> data() throws IOException
  {
    BufferedReader testReader = new BufferedReader(
        new InputStreamReader(MovingAverageQueryTest.class.getResourceAsStream("/queryTests"), StandardCharsets.UTF_8));
    List<String[]> tests = new ArrayList<>();

    for (String line = testReader.readLine(); line != null; line = testReader.readLine()) {
      tests.add(new String[] {line});
    }

    return tests;
  }

  public MovingAverageQueryTest(String yamlFile) throws IOException
  {
    this.yamlFile = yamlFile;

    List<Module> modules = getRequiredModules();
    modules.add(
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("queryTest");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(1);
          binder.bind(QuerySegmentWalker.class).toProvider(Providers.of(new QuerySegmentWalker()
          {
            @Override
            public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
            {
              return new QueryRunner<T>()
              {
                @Override
                @SuppressWarnings("unchecked")
                public Sequence<T> run(QueryPlus queryPlus, Map responseContext)
                {
                  if (query instanceof GroupByQuery) {
                    return (Sequence<T>) Sequences.simple(groupByResults);
                  } else if (query instanceof TimeseriesQuery) {
                    return (Sequence<T>) Sequences.simple(timeseriesResults);
                  }
                  throw new UnsupportedOperationException("unexpected query type " + query.getType());
                }
              };
            }

            @Override
            public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
            {
              return getQueryRunnerForIntervals(query, null);
            }
          }));
        }
    );

    System.setProperty("druid.generic.useDefaultValueForNull", "true");
    System.setProperty("druid.processing.buffer.sizeBytes", "655360");
    Injector baseInjector = GuiceInjectors.makeStartupInjector();
    injector = Initialization.makeInjectorWithModules(baseInjector, modules);

    jsonMapper = injector.getInstance(ObjectMapper.class);
    warehouse = injector.getInstance(QueryToolChestWarehouse.class);
    retryConfig = injector.getInstance(RetryQueryRunnerConfig.class);
    serverConfig = injector.getInstance(ServerConfig.class);

    InputStream is = getClass().getResourceAsStream("/queryTests/" + yamlFile);
    ObjectMapper reader = new ObjectMapper(new YAMLFactory());
    config = reader.readValue(is, TestConfig.class);
  }

  /**
   * Returns the JSON query that should be used in the test.
   *
   * @return The JSON query
   */
  protected String getQueryString()
  {
    return config.query.toString();
  }

  /**
   * Returns the JSON result that should be expected from the query.
   *
   * @return The JSON result
   */
  protected String getExpectedResultString()
  {
    return config.expectedOutput.toString();
  }

  /**
   * Returns the JSON result that the nested groupby query should produce.
   * Either this method or {@link #getTimeseriesResultJson()} must be defined
   * by the subclass.
   *
   * @return The JSON result from the groupby query
   */
  protected String getGroupByResultJson()
  {
    ArrayNode node = config.intermediateResults.get("groupBy");
    return node == null ? null : node.toString();
  }

  /**
   * Returns the JSON result that the nested timeseries query should produce.
   * Either this method or {@link #getGroupByResultJson()} must be defined
   * by the subclass.
   *
   * @return The JSON result from the timeseries query
   */
  protected String getTimeseriesResultJson()
  {
    ArrayNode node = config.intermediateResults.get("timeseries");
    return node == null ? null : node.toString();
  }

  /**
   * Returns the expected query type.
   *
   * @return The Query type
   */
  protected Class<?> getExpectedQueryType()
  {
    return MovingAverageQuery.class;
  }

  protected TypeReference<?> getExpectedResultType()
  {
    return new TypeReference<List<Row>>()
    {
    };
  }

  /**
   * Returns a list of any additional Druid Modules necessary to run the test.
   *
   * @return List of Druid Modules
   */
  protected List<Module> getRequiredModules()
  {
    List<Module> list = new ArrayList<>();

    list.add(new QueryRunnerFactoryModule());
    list.add(new QueryableModule());
    list.add(new DruidProcessingModule());

    return list;
  }

  /**
   * Set up any needed mocks to stub out backend query behavior.
   *
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonParseException
   */
  protected void defineMocks() throws IOException
  {
    groupByResults.clear();
    timeseriesResults.clear();

    if (getGroupByResultJson() != null) {
      groupByResults.addAll(jsonMapper.readValue(getGroupByResultJson(), new TypeReference<List<Row>>()
      {
      }));
    }

    if (getTimeseriesResultJson() != null) {
      timeseriesResults.addAll(jsonMapper.readValue(
          getTimeseriesResultJson(),
          new TypeReference<List<Result<TimeseriesResultValue>>>()
          {
          }
      ));
    }
  }

  /**
   * converts Int to Long, Float to Double in the actual and expected result
   *
   * @param result
   */
  protected List<MapBasedRow> consistentTypeCasting(List<MapBasedRow> result)
  {
    List<MapBasedRow> newResult = new ArrayList<>();
    for (MapBasedRow row : result) {
      final Map<String, Object> event = Maps.newLinkedHashMap((row).getEvent());
      event.forEach((key, value) -> {
        if (Integer.class.isInstance(value)) {
          event.put(key, ((Integer) value).longValue());
        }
        if (Float.class.isInstance(value)) {
          event.put(key, ((Float) value).doubleValue());
        }
      });
      newResult.add(new MapBasedRow(row.getTimestamp(), event));
    }
    
    return newResult;
  }

  /**
   * Validate that the specified query behaves correctly.
   *
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonParseException
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testQuery() throws IOException
  {
    Query<?> query = jsonMapper.readValue(getQueryString(), Query.class);
    assertThat(query, IsInstanceOf.instanceOf(getExpectedQueryType()));

    List<MapBasedRow> expectedResults = jsonMapper.readValue(getExpectedResultString(), getExpectedResultType());
    assertNotNull(expectedResults);
    assertThat(expectedResults, IsInstanceOf.instanceOf(List.class));

    CachingClusteredClient baseClient = new CachingClusteredClient(
        warehouse,
        new TimelineServerView()
        {
          @Override
          public TimelineLookup<String, ServerSelector> getTimeline(DataSource dataSource)
          {
            return null;
          }

          @Override
          public List<ImmutableDruidServer> getDruidServers()
          {
            return null;
          }

          @Override
          public <T> QueryRunner<T> getQueryRunner(DruidServer server)
          {
            return null;
          }

          @Override
          public void registerTimelineCallback(Executor exec, TimelineCallback callback)
          {

          }

          @Override
          public void registerSegmentCallback(Executor exec, SegmentCallback callback)
          {

          }

          @Override
          public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
          {

          }
        },
        MapCache.create(100000),
        jsonMapper,
        new ForegroundCachePopulator(jsonMapper, new CachePopulatorStats(), -1),
        new CacheConfig(),
        new DruidHttpClientConfig()
        {
          @Override
          public long getMaxQueuedBytes()
          {
            return 0L;
          }
        }
    );

    ClientQuerySegmentWalker walker = new ClientQuerySegmentWalker(
        new ServiceEmitter("", "", null)
        {
          @Override
          public void emit(Event event)
          {
          }
        },
        baseClient, warehouse, retryConfig, jsonMapper, serverConfig, null, new CacheConfig()
    );
    final Map<String, Object> responseContext = new HashMap<>();

    defineMocks();

    QueryPlus queryPlus = QueryPlus.wrap(query);
    final Sequence<?> res = query.getRunner(walker).run(queryPlus, responseContext);

    List actualResults = new ArrayList();
    actualResults = (List<MapBasedRow>) res.accumulate(actualResults, Accumulators.list());

    expectedResults = consistentTypeCasting(expectedResults);
    actualResults = consistentTypeCasting(actualResults);

    assertEquals(expectedResults, actualResults);
  }
}
