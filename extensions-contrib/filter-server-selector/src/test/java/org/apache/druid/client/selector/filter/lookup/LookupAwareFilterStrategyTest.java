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

package org.apache.druid.client.selector.filter.lookup;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.selector.ConnectionCountServerSelectorStrategy;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.filter.ComposingServerFilterStrategy;
import org.apache.druid.client.selector.filter.FilterServerSelectorStrategy;
import org.apache.druid.client.selector.filter.ServerFilterStrategy;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.extraction.CascadeExtractionFn;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupsState;
import org.apache.druid.query.lookup.MapLookupExtractorFactory;
import org.apache.druid.query.lookup.RegisteredLookupExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.search.SearchSortSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.lookup.cache.LookupExtractorFactoryMapContainer;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class LookupAwareFilterStrategyTest
{
  public static final String TIME_DIMENSION = "__time";
  public static final String INDEX_METRIC = "index";
  public static final CountAggregatorFactory ROWS_COUNT = new CountAggregatorFactory("rows");
  public static final DoubleSumAggregatorFactory INDEX_DOUBLE_SUM = new DoubleSumAggregatorFactory(
      "index",
      INDEX_METRIC
  );
  public static final HyperUniquesAggregatorFactory QUALITY_UNIQUES = new HyperUniquesAggregatorFactory(
      "uniques",
      "quality_uniques"
  );
  public static final List<AggregatorFactory> COMMON_DOUBLE_AGGREGATORS = Arrays.asList(
      ROWS_COUNT,
      INDEX_DOUBLE_SUM,
      QUALITY_UNIQUES
  );
  
  private LookupAwareFilterStrategy buildFilterStrategy(
      Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>> lookups)
      throws InterruptedException
  {
    LookupsCoordinatorClient client = EasyMock.createMock(LookupsCoordinatorClient.class);
    EasyMock.expect(client.fetchLookupNodeStatus()).andReturn(lookups).anyTimes();
    EasyMock.replay(client);

    LookupStatusView view = new LookupStatusView(client, new LookupStatusViewConfig());
    view.start();
    view.stop();

    return new LookupAwareFilterStrategy(view);
  }

  @Test
  public void testWithFilteringStrategy() throws InterruptedException
  {
    LookupAwareFilterStrategy strategy = buildFilterStrategy(constructLookups());
    List<ServerFilterStrategy> filters = new ArrayList<>();
    filters.add(strategy);
    Set<QueryableDruidServer> servers = createServerTopology();
    FilterServerSelectorStrategy filteringStrategy = new FilterServerSelectorStrategy(
        new ConnectionCountServerSelectorStrategy(), new ComposingServerFilterStrategy(filters));

    Query query = constructTopNQuery("lookup0");
    QueryableDruidServer selected = filteringStrategy.pick(query, servers, null);
    Assert.assertTrue(selected.getServer().getName().equals("test1"));

    query = constructTopNQuery("lookup2");
    selected = filteringStrategy.pick(query, servers, null);
    Assert.assertTrue(selected != null);

    selected = filteringStrategy.pick(servers, null);
    Assert.assertTrue(selected != null);
  }

  @Test
  public void testSimpleLookup() throws InterruptedException
  {
    LookupAwareFilterStrategy strategy = buildFilterStrategy(constructLookups());
    Set<QueryableDruidServer> servers = createServerTopology();

    ArrayList<Function<String, Query>> creators = new ArrayList<>();
    creators.add(LookupAwareFilterStrategyTest::constructVirtualColumnQuery);
    creators.add(LookupAwareFilterStrategyTest::constructTopNQuery);
    creators.add(LookupAwareFilterStrategyTest::constructGroupByQuery);
    creators.add(LookupAwareFilterStrategyTest::constructSearchQuery);
    creators.add(LookupAwareFilterStrategyTest::constructScanQueryWithJoin);
    creators.add(LookupAwareFilterStrategyTest::constructQueryWithContextLookups);

    for (Function<String, Query> queryCreator : creators) {
      testLookup(strategy, servers, "lookup0", queryCreator, 2);
      testLookup(strategy, servers, "lookup1", queryCreator, 3);
      testLookup(strategy, servers, "lookup2", queryCreator, 4);
      testLookup(strategy, servers, "lookup0", queryCreator, 2);
    }
    //constructCascade
    

    testLookup(strategy, servers, "lookup0", lookup -> constructScanQueryWithJoinNested(lookup, "lookup1"), 3);
    testLookup(strategy, servers, "lookup1", lookup -> constructScanQueryWithJoinNested(lookup, "lookup0"), 3);
    testLookup(strategy, servers, "lookup0", lookup -> constructScanQueryWithJoinNested(lookup, "lookup0"), 2);
    testLookup(strategy, servers, "lookup0", lookup -> constructScanQueryWithJoinNested(lookup, "lookup2"), 4);
    testLookup(strategy, servers, "lookup2", lookup -> constructScanQueryWithJoinNested(lookup, "lookup0"), 4);
    
    testLookup(strategy, servers, "lookup0", lookup -> constructCascade(lookup, "lookup1"), 3);
    testLookup(strategy, servers, "lookup2", lookup -> constructCascade(lookup, "lookup0"), 4);

  }

  private void testLookup(LookupAwareFilterStrategy strategy, Set<QueryableDruidServer> servers, String lookup,
      Function<String, Query> creator,
      int sizeDifference)
  {
    Query query = creator.apply(lookup);
    Set<QueryableDruidServer> filteredServers = strategy.filter(query, servers);
    Assert.assertTrue(servers.size() - filteredServers.size() == sizeDifference);
  }

  public static Set<QueryableDruidServer> createServerTopology()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    EasyMock.expect(client.getNumOpenConnections()).andReturn(0).anyTimes();

    DirectDruidClient connectedClient = EasyMock.createMock(DirectDruidClient.class);
    EasyMock.expect(connectedClient.getNumOpenConnections()).andReturn(2).anyTimes();

    EasyMock.replay(client, connectedClient);

    Set<QueryableDruidServer> servers = new HashSet<QueryableDruidServer>();
    servers.add(new QueryableDruidServer(
        new DruidServer("test0", "localhost:8080", null, 0, ServerType.REALTIME, DruidServer.DEFAULT_TIER, 0),
        connectedClient));
    servers.add(new QueryableDruidServer(
        new DruidServer("test1", "localhost:8081", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client));
    servers.add(new QueryableDruidServer(
        new DruidServer("test2", "localhost:8082", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client));
    servers.add(new QueryableDruidServer(
        new DruidServer("test3", "localhost:8083", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client));
    return servers;
  }

  public static GroupByQuery constructVirtualColumnQuery(String lookup)
  {
    Map<String, String> countryCodeToNameMap = new HashMap<String, String>();
    NullHandling.initializeForTests();
    final ExprMacroTable exprMacroTable = new ExprMacroTable(
        ImmutableList.of(new LookupExprMacro(
            new LookupExtractorFactoryContainerProvider()
            {
              @Override
              public Set<String> getAllLookupNames()
              {
                return ImmutableSet.of(lookup);
              }

              @Override
              public Optional<LookupExtractorFactoryContainer> get(String lookupName)
              {
                if (lookup.equals(lookupName)) {
                  return Optional.of(
                      new LookupExtractorFactoryContainer(
                          "0",
                          new MapLookupExtractorFactory(countryCodeToNameMap, false)));
                } else {
                  return Optional.empty();
                }
              }
            })));
    ExpressionVirtualColumn evc = new ExpressionVirtualColumn(
        lookup,
        "lookup(countryIsoCode, '" + lookup + "')",
        ValueType.STRING,
        exprMacroTable);
    return GroupByQuery.builder()
        .setDataSource("dummy")
        .setGranularity(Granularities.ALL)
        .setInterval("2000/2001")
        .addDimension(new DefaultDimensionSpec("bar", "bar", ValueType.FLOAT))
        .addDimension(new DefaultDimensionSpec("baz", "baz", ValueType.STRING))
        .setVirtualColumns(evc)
        .build();
  }

  public static TopNQuery constructTopNQuery(String lookup)
  {
    RegisteredLookupExtractionFn fn = new RegisteredLookupExtractionFn(
        null,
        lookup,
        true,
        null,
        false,
        false);
    TopNQuery expectedQuery = new TopNQueryBuilder()
        .dataSource("testing")
        .granularity(Granularities.ALL)
        .dimension(
            new ExtractionDimensionSpec(
                "country_code",
                "country_name",
                fn))
        .metric(new NumericTopNMetricSpec("index"))
        .threshold(2)
        .intervals(
            new MultipleIntervalSegmentSpec(
                Collections.singletonList(
                    new Interval(DateTimes.of("2012-01-01"), DateTimes.of("2012-01-01").plusHours(1))))
            )
        .context(new HashMap<String, Object>())
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    COMMON_DOUBLE_AGGREGATORS,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")))))
        .build();
    return expectedQuery;
  }

  public static ScanQuery constructScanQueryWithJoin(String lookup)
  {
    JoinDataSource joinDataSource = JoinDataSource.create(
        new TableDataSource("foo"),
        new LookupDataSource(lookup),
        lookup + ".",
        "x == \"" + lookup + ".x\"",
        JoinType.INNER,
        ExprMacroTable.nil());

    return Druids.newScanQueryBuilder()
        .order(ScanQuery.Order.ASCENDING)
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
        .dataSource(joinDataSource)
        .intervals(new MultipleIntervalSegmentSpec(
            Collections.singletonList(
                new Interval(DateTimes.of("2012-01-01"), DateTimes.of("2012-01-01").plusHours(1)))))
        .build();
  }

  public static ScanQuery constructScanQueryWithJoinNested(String lookup, String lookup2)
  {
    JoinDataSource joinDataSource = JoinDataSource.create(
        new TableDataSource("foo"),
        new LookupDataSource(lookup),
        lookup + ".",
        "x == \"" + lookup + ".x\"",
        JoinType.INNER,
        ExprMacroTable.nil());

    JoinDataSource joinDataSource2 = JoinDataSource.create(
        joinDataSource,
        new LookupDataSource(lookup2),
        lookup2 + ".",
        "y == \"" + lookup + ".y\"",
        JoinType.INNER,
        ExprMacroTable.nil());

    JoinDataSource joinDataSource3 = JoinDataSource.create(
        new TableDataSource("foo2"),
        joinDataSource2,
        lookup + ".",
        "x == \"" + lookup + ".x\"",
        JoinType.INNER,
        ExprMacroTable.nil());

    return Druids.newScanQueryBuilder()
        .order(ScanQuery.Order.ASCENDING)
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
        .dataSource(joinDataSource3)
        .intervals(new MultipleIntervalSegmentSpec(
            Collections.singletonList(
                new Interval(DateTimes.of("2012-01-01"), DateTimes.of("2012-01-01").plusHours(1)))))
        .build();
  }

  public static GroupByQuery constructGroupByQuery(String lookup)
  {
    RegisteredLookupExtractionFn fn = new RegisteredLookupExtractionFn(
        null,
        lookup,
        true,
        null,
        false,
        false);
    return GroupByQuery.builder()
        .setDataSource("dummy")
        .setGranularity(Granularities.ALL)
        .setInterval("2000/2001")
        .addDimension(new ExtractionDimensionSpec(
            "country_code",
            "country_name",
            fn))
        .addDimension(new DefaultDimensionSpec("bar", "bar", ValueType.FLOAT))
        .addDimension(new DefaultDimensionSpec("baz", "baz", ValueType.STRING))
        .build();
  }
  
  public static GroupByQuery constructCascade(String lookup, String lookup2)
  {
    RegisteredLookupExtractionFn fn = new RegisteredLookupExtractionFn(
        null,
        lookup,
        true,
        null,
        false,
        false);
    RegisteredLookupExtractionFn fn2 = new RegisteredLookupExtractionFn(
        null,
        lookup2,
        true,
        null,
        false,
        false);
    return GroupByQuery.builder()
        .setDataSource("dummy")
        .setGranularity(Granularities.ALL)
        .setInterval("2000/2001")
        .addDimension(new ExtractionDimensionSpec(
            "country_code",
            "country_name",
            new CascadeExtractionFn(new ExtractionFn[] {fn, fn2})))
        .addDimension(new DefaultDimensionSpec("bar", "bar", ValueType.FLOAT))
        .addDimension(new DefaultDimensionSpec("baz", "baz", ValueType.STRING))
        .build();
  }

  public static GroupByQuery constructQueryWithContextLookups(String lookup)
  {
    Set<String> lookups = new HashSet<String>();
    lookups.add(lookup);
    Map<String, Object> context = new HashMap<String, Object>();
    context.put(LookupAwareFilterStrategy.LOOKUPS_CONTEXT_KEY, lookups);
    return GroupByQuery.builder()
        .setDataSource("dummy")
        .setGranularity(Granularities.ALL)
        .setInterval("2000/2001")
        .addDimension(new DefaultDimensionSpec("bar", "bar", ValueType.FLOAT))
        .addDimension(new DefaultDimensionSpec("baz", "baz", ValueType.STRING))
        .setContext(context)
        .build();
  }

  public static SearchQuery constructSearchQuery(String lookup)
  {
    return Druids.newSearchQueryBuilder()
        .dataSource("testing")
        .granularity(Granularities.ALL)
        .intervals(new MultipleIntervalSegmentSpec(
            Collections.singletonList(Intervals.of("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z"))
            ))
        .dimensions(
            new ExtractionDimensionSpec(
                "country_code",
                "country_name",
                new RegisteredLookupExtractionFn(
                    null,
                    lookup,
                    true,
                    null,
                    false,
                    false)))
        .sortSpec(new SearchSortSpec(StringComparators.STRLEN))
        .query("e")
        .context(ImmutableMap.of())
        .build();
  }

  public static Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>> constructLookups()
  {
    Map<String, LookupExtractorFactoryMapContainer> server0 = new HashMap<String, LookupExtractorFactoryMapContainer>();
    server0.put("lookup0", EasyMock.createMock(LookupExtractorFactoryMapContainer.class));

    Map<String, LookupExtractorFactoryMapContainer> server1 = new HashMap<String, LookupExtractorFactoryMapContainer>();
    server1.put("lookup0", EasyMock.createMock(LookupExtractorFactoryMapContainer.class));
    server1.put("lookup1", EasyMock.createMock(LookupExtractorFactoryMapContainer.class));

    Map<String, LookupExtractorFactoryMapContainer> server2 = new HashMap<String, LookupExtractorFactoryMapContainer>();

    Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> defaultTier = new HashMap<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>();
    defaultTier.put(HostAndPort.fromString("localhost:8080"),
        new LookupsState<LookupExtractorFactoryMapContainer>(server0, null, null));
    defaultTier.put(HostAndPort.fromString("localhost:8081"),
        new LookupsState<LookupExtractorFactoryMapContainer>(server1, null, null));
    defaultTier.put(HostAndPort.fromString("localhost:8082"),
        new LookupsState<LookupExtractorFactoryMapContainer>(server2, null, null));

    Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>> lookups = new HashMap<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>>();
    lookups.put("__default", defaultTier);
    return lookups;
  }

}
