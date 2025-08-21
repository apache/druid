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

package org.apache.druid.testing.embedded.auth;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.IndexTaskTest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class BasicAuthIndexingTest extends IndexTaskTest
{
  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .addResource(new EmbeddedBasicAuthResource())
        .useLatchableEmitter()
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(historical)
        .addServer(broker)
        .addServer(new EmbeddedRouter())
        .addCommonProperty("druid.indexer.autoscale.doAutoscale", "true");
  }

  @Test
  public void test_getScalingStats_redirectFromCoordinatorToOverlord()
  {
    final List<Object> response = cluster.callApi().serviceClient().onLeaderCoordinator(
        mapper -> new RequestBuilder(HttpMethod.GET, "/druid/indexer/v1/scaling"),
        new TypeReference<>() {}
    );
    Assertions.assertNotNull(response);
    Assertions.assertTrue(response.isEmpty());
  }
}
