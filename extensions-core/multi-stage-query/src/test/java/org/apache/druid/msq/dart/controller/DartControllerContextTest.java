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
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.MemoryIntrospectorImpl;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
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

import java.util.List;
import java.util.stream.Collectors;

public class DartControllerContextTest
{
  private static final List<DruidServerMetadata> SERVERS = ImmutableList.of(
      new DruidServerMetadata("no", "localhost:1001", null, 1, ServerType.HISTORICAL, "__default", 2), // plaintext
      new DruidServerMetadata("no", null, "localhost:1002", 1, ServerType.HISTORICAL, "__default", 1), // TLS
      new DruidServerMetadata("no", "localhost:1003", null, 1, ServerType.REALTIME, "__default", 0)
  );
  private static final DruidNode SELF_NODE = new DruidNode("none", "localhost", false, 8080, -1, true, false);
  private static final String QUERY_ID = "abc";

  /**
   * Context returned by {@link #query}. Overrides "maxConcurrentStages".
   */
  private QueryContext queryContext =
      QueryContext.of(ImmutableMap.of(MultiStageQueryContext.CTX_MAX_CONCURRENT_STAGES, 3));
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
  private MSQSpec querySpec;

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
    Mockito.when(querySpec.getQuery()).thenReturn(query);
    Mockito.when(querySpec.getDestination()).thenReturn(TaskReportMSQDestination.instance());
    Mockito.when(query.context()).thenReturn(queryContext);
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    mockCloser.close();
  }

  @Test
  public void test_queryKernelConfig()
  {
    final DartControllerContext controllerContext =
        new DartControllerContext(null, null, SELF_NODE, null, memoryIntrospector, serverView, null);
    final ControllerQueryKernelConfig queryKernelConfig = controllerContext.queryKernelConfig(QUERY_ID, querySpec);

    Assertions.assertFalse(queryKernelConfig.isFaultTolerant());
    Assertions.assertFalse(queryKernelConfig.isDurableStorage());
    Assertions.assertEquals(3, queryKernelConfig.getMaxConcurrentStages());
    Assertions.assertEquals(TaskReportMSQDestination.instance(), queryKernelConfig.getDestination());
    Assertions.assertTrue(queryKernelConfig.isPipeline());

    // Check workerIds after sorting, because they've been shuffled.
    Assertions.assertEquals(
        ImmutableList.of(
            // Only the HISTORICAL servers
            WorkerId.fromDruidServerMetadata(SERVERS.get(0), QUERY_ID).toString(),
            WorkerId.fromDruidServerMetadata(SERVERS.get(1), QUERY_ID).toString()
        ),
        queryKernelConfig.getWorkerIds().stream().sorted().collect(Collectors.toList())
    );
  }
}
