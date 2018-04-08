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
package io.druid.query.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.client.BatchServerInventoryView;
import io.druid.client.BrokerSegmentWatcherConfig;
import io.druid.client.BrokerServerView;
import io.druid.client.DruidServer;
import io.druid.client.selector.HighestPriorityTierSelectorStrategy;
import io.druid.client.selector.RandomServerSelectorStrategy;
import io.druid.curator.CuratorTestBase;
import io.druid.indexing.materializedview.DerivativeDataSourceMetadata;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.http.client.HttpClient;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.metadata.TestDerbyConnector;
import io.druid.query.Query;
import static io.druid.query.QueryRunnerTestHelper.allGran;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryWatcher;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.segment.TestHelper;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class DatasourceOptimizerTest extends CuratorTestBase 
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private TestDerbyConnector derbyConnector;
  private DerivativesManager derivativesManager;
  private DruidServer druidServer;
  private ObjectMapper jsonMapper;
  private ZkPathsConfig zkPathsConfig;
  private DatasourceOptimizer optimizer;
  private MaterializedViewConfig viewConfig;
  private IndexerSQLMetadataStorageCoordinator metadataStorageCoordinator;
  private BatchServerInventoryView baseView;
  private BrokerServerView brokerServerView;
  
  @Before
  public void setUp() throws Exception
  {
    derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createSegmentTable();
    viewConfig = new MaterializedViewConfig();
    jsonMapper = TestHelper.makeJsonMapper();
    jsonMapper.registerSubtypes(new NamedType(DerivativeDataSourceMetadata.class, "view"));
    metadataStorageCoordinator = EasyMock.createMock(IndexerSQLMetadataStorageCoordinator.class);
    derivativesManager = new DerivativesManager(
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
    optimizer = new DatasourceOptimizer(brokerServerView);
  }
  
  @After
  public void tearDown() throws IOException 
  {
    baseView.stop();
    tearDownServerAndCurator();
  }
  
  @Test(timeout = 10 * 1000)
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
        ImmutableList.<String>of(
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
            metadataStorageCoordinator.announceHistoricalSegments(Sets.newHashSet(segment));
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
        ImmutableList.<String>of(
            "2011-04-01/2011-04-02",
            "2011-04-02/2011-04-03",
            "2011-04-03/2011-04-04"
        ),
        interval -> {
          final DataSegment segment = createDataSegment("derivative", interval, "v1", Lists.newArrayList("dim1", "dim2", "dim3"), 1024);
          try {
            metadataStorageCoordinator.announceHistoricalSegments(Sets.newHashSet(segment));
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
    while (DerivativesManager.getAllDerivatives().isEmpty()) {
      TimeUnit.SECONDS.sleep(1L);
    }
    // build user query
    TopNQuery userQuery = new TopNQueryBuilder()
        .dataSource("base")
        .granularity(allGran)
        .dimension("dim1")
        .metric("cost")
        .threshold(4)
        .intervals("2011-04-01/2011-04-06")
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                new LongSumAggregatorFactory("cost", "cost")
            )
        )
        .build();
    
    List<Query> expectedQueryAfterOptimizing = Lists.newArrayList(
        new TopNQueryBuilder()
            .dataSource("derivative")
            .granularity(allGran)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleIntervalSegmentSpec(Lists.newArrayList(Intervals.of("2011-04-01/2011-04-04"))))
            .aggregators(
                Lists.<AggregatorFactory>newArrayList(
                    new LongSumAggregatorFactory("cost", "cost")
                )
            )
            .build(),
        new TopNQueryBuilder()
            .dataSource("base")
            .granularity(allGran)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleIntervalSegmentSpec(Lists.newArrayList(Intervals.of("2011-04-04/2011-04-06"))))
            .aggregators(
                Lists.<AggregatorFactory>newArrayList(
                    new LongSumAggregatorFactory("cost", "cost")
                )
            )
            .build()
    );
    Assert.assertEquals(expectedQueryAfterOptimizing, DatasourceOptimizer.optimize(userQuery));
    derivativesManager.stop();
  }
  
  private DataSegment createDataSegment(String name, String intervalStr, String version, List<String> dims, long size)
  {
    return DataSegment.builder()
        .dataSource(name)
        .interval(Intervals.of(intervalStr))
        .loadSpec(
            ImmutableMap.<String, Object>of(
                "type",
                "local",
                "path",
                "somewhere"
            )
        )
        .version(version)
        .dimensions(dims)
        .metrics(ImmutableList.<String>of("cost"))
        .shardSpec(NoneShardSpec.instance())
        .binaryVersion(9)
        .size(size)
        .build();
  }

  private void setupViews() throws Exception
  {
    baseView = new BatchServerInventoryView(
        zkPathsConfig,
        curator,
        jsonMapper,
        Predicates.alwaysTrue()
    )
    {
      @Override
      public void registerSegmentCallback(Executor exec, final SegmentCallback callback)
      {
        super.registerSegmentCallback(
            exec, new SegmentCallback()
            {
              @Override
              public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
              {
                CallbackAction res = callback.segmentAdded(server, segment);
                return res;
              }

              @Override
              public CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
              {
                CallbackAction res = callback.segmentRemoved(server, segment);
                return res;
              }

              @Override
              public CallbackAction segmentViewInitialized()
              {
                CallbackAction res = callback.segmentViewInitialized();
                return res;
              }
            }
        );
      }
    };

    brokerServerView = new BrokerServerView(
        EasyMock.createMock(QueryToolChestWarehouse.class),
        EasyMock.createMock(QueryWatcher.class),
        getSmileMapper(),
        EasyMock.createMock(HttpClient.class),
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
    final ObjectMapper retVal = new DefaultObjectMapper(smileFactory);
    retVal.getFactory().setCodec(retVal);
    return retVal;
  }
  
}
