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

package org.apache.druid.msq.dart.controller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.dart.worker.DartWorkerService;
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.MemoryIntrospectorImpl;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.druid.msq.test.TestDartControllerContextFactoryImpl;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DartControllerContextTest
{
  private static final List<DruidServerMetadata> SERVERS = ImmutableList.of(
      new DruidServerMetadata("no", "localhost:1001", null, 1, null, ServerType.HISTORICAL, "__default", 2), // plaintext
      new DruidServerMetadata("no", null, "localhost:1002", 1, null, ServerType.HISTORICAL, "__default", 1), // TLS
      new DruidServerMetadata("no", "localhost:1003", null, 1, null, ServerType.REALTIME, "__default", 0)
  );
  private static final DruidNode SELF_NODE = new DruidNode("none", "localhost", false, 8080, -1, true, false);
  private static final String QUERY_ID = "abc";

  /**
   * Context returned by {@link #query}. Overrides "maxConcurrentStages".
   */
  private final QueryContext queryContext =
      QueryContext.of(
          ImmutableMap.of(
              MultiStageQueryContext.CTX_MAX_CONCURRENT_STAGES, 3,
              QueryContexts.CTX_DART_QUERY_ID, QUERY_ID
          )
      );
  private MemoryIntrospector memoryIntrospector;
  private AutoCloseable mockCloser;

  /**
   * Server view that returns {@link #SERVERS}.
   */
  @Mock
  private BrokerServerView serverView;

  /**
   * Query spec that exists mainly to test {@link DartControllerContext#queryKernelConfig}.
   */
  @Mock
  private LegacyMSQSpec querySpec;

  /**
   * Query returned by {@link #querySpec}.
   */
  @Mock
  private Query query;

  @BeforeEach
  public void setUp()
  {
    mockCloser = MockitoAnnotations.openMocks(this);
    memoryIntrospector = new MemoryIntrospectorImpl(100_000_000, 0.75, 1, 1, null);
    Mockito.when(serverView.getDruidServerMetadatas()).thenReturn(SERVERS);
    Mockito.when(querySpec.getDestination()).thenReturn(TaskReportMSQDestination.instance());
    Mockito.when(querySpec.getContext()).thenReturn(queryContext);
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    mockCloser.close();
  }

  @Test
  public void test_queryKernelConfig_allHistoricalsAdvertiseDartWorker()
  {
    // Both HISTORICALs advertise a Dart worker: enroll both, matching pre-capability behavior.
    final ControllerQueryKernelConfig queryKernelConfig =
        makeControllerContext(discoveryOf(SERVERS.get(0), SERVERS.get(1))).queryKernelConfig(querySpec);

    assertCommonKernelConfig(queryKernelConfig);
    Assertions.assertEquals(
        ImmutableList.of(
            WorkerId.fromDruidServerMetadata(SERVERS.get(0), QUERY_ID).toString(),
            WorkerId.fromDruidServerMetadata(SERVERS.get(1), QUERY_ID).toString()
        ),
        sortedWorkerIds(queryKernelConfig)
    );
  }

  @Test
  public void test_queryKernelConfig_onlySomeHistoricalsAdvertiseDartWorker()
  {
    // Only the first HISTORICAL advertises a Dart worker: the Dart-disabled one is excluded.
    final ControllerQueryKernelConfig queryKernelConfig =
        makeControllerContext(discoveryOf(SERVERS.get(0))).queryKernelConfig(querySpec);

    assertCommonKernelConfig(queryKernelConfig);
    Assertions.assertEquals(
        ImmutableList.of(WorkerId.fromDruidServerMetadata(SERVERS.get(0), QUERY_ID).toString()),
        sortedWorkerIds(queryKernelConfig)
    );
  }

  @Test
  public void test_queryKernelConfig_noHistoricalsAdvertiseDartWorker_failsFast()
  {
    // Historicals are present in the server view but none advertise a Dart worker so we must fail fast
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> makeControllerContext(discoveryOf()).queryKernelConfig(querySpec)
    );

    Assertions.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
    Assertions.assertTrue(e.getMessage().contains("No Dart workers are available"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("druid.msq.dart.enabled"), e.getMessage());
  }

  @Test
  public void test_queryKernelConfig_noHistoricalsAtAll_failsFast()
  {
    // No historical servers at all: still fail fast, but with a descriptive message about the cause
    Mockito.when(serverView.getDruidServerMetadatas())
           .thenReturn(ImmutableList.of(SERVERS.get(2))); // realtime only

    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> makeControllerContext(discoveryOf()).queryKernelConfig(querySpec)
    );

    Assertions.assertTrue(e.getMessage().contains("no Historicals are currently available"), e.getMessage());
  }

  private DartControllerContext makeControllerContext(final DruidNodeDiscovery dartWorkerDiscovery)
  {
    return new DartControllerContext(
        null,
        null,
        SELF_NODE,
        null,
        memoryIntrospector,
        serverView,
        List.of(),
        null,
        queryContext,
        dartWorkerDiscovery
    );
  }

  private static void assertCommonKernelConfig(final ControllerQueryKernelConfig queryKernelConfig)
  {
    Assertions.assertFalse(queryKernelConfig.isFaultTolerant());
    Assertions.assertFalse(queryKernelConfig.isDurableStorage());
    Assertions.assertEquals(3, queryKernelConfig.getMaxConcurrentStages());
    Assertions.assertEquals(TaskReportMSQDestination.instance(), queryKernelConfig.getDestination());
    Assertions.assertTrue(queryKernelConfig.isPipeline());
  }

  /**
   * The workerIds are shuffled by {@link DartControllerContext#queryKernelConfig}, so sort before comparing.
   */
  private static List<String> sortedWorkerIds(final ControllerQueryKernelConfig queryKernelConfig)
  {
    return queryKernelConfig.getWorkerIds().stream().sorted().collect(Collectors.toList());
  }

  /**
   * A {@link DruidNodeDiscovery} that reports the given servers as Historicals advertising a {@link DartWorkerService}.
   */
  private static DruidNodeDiscovery discoveryOf(final DruidServerMetadata... servers)
  {
    final List<DiscoveryDruidNode> nodes =
        Arrays.stream(servers)
              .map(TestDartControllerContextFactoryImpl::historicalDartWorkerNode)
              .collect(Collectors.toList());
    final DruidNodeDiscovery discovery = Mockito.mock(DruidNodeDiscovery.class);
    Mockito.when(discovery.getAllNodes()).thenReturn(nodes);
    return discovery;
  }
}
