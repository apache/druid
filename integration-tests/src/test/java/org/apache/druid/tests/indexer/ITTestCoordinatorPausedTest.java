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

package org.apache.druid.tests.indexer;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITTestCoordinatorPausedTest extends AbstractITBatchIndexTest
{
  private static final Logger LOG = new Logger(ITTestCoordinatorPausedTest.class);
  private static final String INDEX_DATASOURCE = "wikipedia_index_test";
  private static final String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final CoordinatorDynamicConfig DYNAMIC_CONFIG_PAUSED =
      CoordinatorDynamicConfig.builder().withPauseCoordination(true).build();
  private static final CoordinatorDynamicConfig DYNAMIC_CONFIG_DEFAULT =
      CoordinatorDynamicConfig.builder().build();

  @Inject
  CoordinatorResourceTestClient coordinatorClient;

  @Test
  public void testCoordinatorPause() throws Exception
  {
    try (
        final Closeable ignored1 = unloader(INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix())
    ) {
      coordinatorClient.postDynamicConfig(DYNAMIC_CONFIG_PAUSED);
      doIndexTest(
          INDEX_DATASOURCE,
          INDEX_TASK,
          INDEX_QUERIES_RESOURCE,
          false,
          false,
          false
      );
      TimeUnit.MINUTES.sleep(3);
      if (coordinatorClient.areSegmentsLoaded(INDEX_DATASOURCE)) {
        throw new IllegalStateException("Segments Were Loaded Early!");
      }
      coordinatorClient.postDynamicConfig(DYNAMIC_CONFIG_DEFAULT);
      ITRetryUtil.retryUntilTrue(
          () -> coordinator.areSegmentsLoaded(INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix()), "Segment Load"
      );
    }
  }
}
