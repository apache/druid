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

package org.apache.druid.query.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.client.BatchServerInventoryView;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.indexing.materializedview.DerivativeDataSourceMetadata;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class DatasourceOptimizerTest extends CuratorTestBase
{
  static {
    NullHandling.initializeForTests();
  }

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private DerivativeDataSourceManager derivativesManager;
  private DruidServer druidServer;
  private ObjectMapper jsonMapper;
  private ZkPathsConfig zkPathsConfig;
  private DataSourceOptimizer optimizer;
  private IndexerSQLMetadataStorageCoordinator metadataStorageCoordinator;
  private BatchServerInventoryView baseView;
  private BrokerServerView brokerServerView;

  @Before
  public void setUp() throws Exception
  {
    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createSegmentTable();
    MaterializedViewConfig viewConfig = new MaterializedViewConfig();
    jsonMapper = TestHelper.makeJsonMapper();
    jsonMapper.registerSubtypes(new NamedType(DerivativeDataSourceMetadata.class, "view"));
    metadataStorageCoordinator = EasyMock.createMock(IndexerSQLMetadataStorageCoordinator.class);
    derivativesManager = new DerivativeDataSourceManager(
        viewConfig,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        jsonMapper,
        derbyConnector
    );
    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        jsonMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector
    );

    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();

    zkPathsConfig = new ZkPathsConfig();
    setupViews();

    druidServer = new DruidServer(
        "localhost:1234",
        "localhost:1234",
        null,
        10000000L,
        ServerType.HISTORICAL,
        "default_tier",
        0
    );
    setupZNodeForServer(druidServer, new ZkPathsConfig(), jsonMapper);
    optimizer = new DataSourceOptimizer(brokerServerView);
  }

  @After
  public void tearDown() throws IOException
  {
    baseView.stop();
    tearDownServerAndCurator();
  }

  @Test(timeout = 60_000L)
  public void testOptimize() throws InterruptedException
  {
    // insert datasource metadata
    String dataSource = "derivative";
    String baseDataSource = "base";
    Set<String> dims = Sets.newHashSet("dim1", "dim2", "dim3");
    Set<String> metrics = Sets.newHashSet("cost");
    DerivativeDataSourceMetadata metadata = new DerivativeDataSourceMetadata(baseDataSource, dims, metrics);
    metadataStorageCoordinator.insertDataSourceMetadata(dataSource, metadata);
    // insert base datasource segments
    List<Boolean> baseResult = Lists.transform(
        ImmutableList.of(
            "2011-04-01/2011-04-02",
            "2011-04-02/2011-04-03",
            "2011-04-03/2011-04-04",
            "2011-04-04/2011-04-05",
            "2011-04-05/2011-04-06"
        ),
        interval -> {
          final DataSegment segment = createDataSegment(
              "base",
              interval,
              "v1",
              Lists.newArrayList("dim1", "dim2", "dim3", "dim4"),
              1024 * 1024
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment));
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );
    // insert derivative segments
    List<Boolean> derivativeResult = Lists.transform(
        ImmutableList.of(
            "2011-04-01/2011-04-02",
            "2011-04-02/2011-04-03",
            "2011-04-03/2011-04-04"
        ),
        interval -> {
          final DataSegment segment = createDataSegment(
              "derivative",
              interval,
              "v1",
              Lists.newArrayList("dim1", "dim2", "dim3"),
              1024
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment));
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );
    Assert.assertFalse(baseResult.contains(false));
    Assert.assertFalse(derivativeResult.contains(false));
    derivativesManager.start();
    while (DerivativeDataSourceManager.getAllDerivatives().isEmpty()) {
      TimeUnit.SECONDS.sleep(1L);
    }
    // build user query
    TopNQuery userQuery = new TopNQueryBuilder()
        .dataSource("base")
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("dim1")
        .metric("cost")
        .threshold(4)
        .intervals("2011-04-01/2011-04-06")
        .aggregators(new LongSumAggregatorFactory("cost", "cost"))
        .build();

    List<Query> expectedQueryAfterOptimizing = Lists.newArrayList(
        new TopNQueryBuilder()
            .dataSource("derivative")
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-01/2011-04-04"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource("base")
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-04/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build()
    );
    Assert.assertEquals(expectedQueryAfterOptimizing, optimizer.optimize(userQuery));
    derivativesManager.stop();
  }

  private DataSegment createDataSegment(String name, String intervalStr, String version, List<String> dims, long size)
  {
    return DataSegment.builder()
                      .dataSource(name)
                      .interval(Intervals.of(intervalStr))
                      .loadSpec(
                          ImmutableMap.of(
                              "type",
                              "local",
                              "path",
                              "somewhere"
                          )
                      )
                      .version(version)
                      .dimensions(dims)
                      .metrics(ImmutableList.of("cost"))
                      .shardSpec(NoneShardSpec.instance())
                      .binaryVersion(9)
                      .size(size)
                      .build();
  }

  private void setupViews() throws Exception
  {
    baseView = new BatchServerInventoryView(zkPathsConfig, curator, jsonMapper, Predicates.alwaysTrue(), "test")
    {
      @Override
      public void registerSegmentCallback(Executor exec, final SegmentCallback callback)
      {
        super.registerSegmentCallback(
            exec,
            new SegmentCallback()
            {
              @Override
              public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
              {
                return callback.segmentAdded(server, segment);
              }

              @Override
              public CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
              {
                return callback.segmentRemoved(server, segment);
              }

              @Override
              public CallbackAction segmentViewInitialized()
              {
                return callback.segmentViewInitialized();
              }

              @Override
              public CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
              {
                return CallbackAction.CONTINUE;
              }
            }
        );
      }
    };

    DirectDruidClientFactory druidClientFactory = new DirectDruidClientFactory(
        new NoopServiceEmitter(),
        EasyMock.createMock(QueryToolChestWarehouse.class),
        EasyMock.createMock(QueryWatcher.class),
        getSmileMapper(),
        EasyMock.createMock(HttpClient.class)
    );

    brokerServerView = new BrokerServerView(
        druidClientFactory,
        baseView,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        new NoopServiceEmitter(),
        new BrokerSegmentWatcherConfig()
    );
    baseView.start();
  }

  private ObjectMapper getSmileMapper()
  {
    final SmileFactory smileFactory = new SmileFactory();
    smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
    smileFactory.delegateToTextual(true);
    final ObjectMapper retVal = new DefaultObjectMapper(smileFactory, "broker");
    retVal.getFactory().setCodec(retVal);
    return retVal;
  }
}
