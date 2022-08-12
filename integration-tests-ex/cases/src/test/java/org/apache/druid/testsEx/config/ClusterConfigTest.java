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

package org.apache.druid.testsEx.config;

import org.apache.druid.testsEx.config.ClusterConfig.ClusterType;
import org.apache.druid.testsEx.config.ResolvedService.ResolvedZk;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Sanity check of an example YAML config file using the Java
 * deserialization classes.
 */
public class ClusterConfigTest
{
  @Test
  public void testYaml()
  {
    ClusterConfig config = ClusterConfig.loadFromResource("/config-test/test.yaml");
    // Uncomment this line to see the full config with includes resolved.
    //System.out.println(config.resolveIncludes());

    ResolvedConfig resolved = config.resolve();
    assertEquals(ClusterType.docker, resolved.type());
    assertEquals(ResolvedConfig.DEFAULT_READY_TIMEOUT_SEC, resolved.readyTimeoutSec());
    assertEquals(ResolvedConfig.DEFAULT_READY_POLL_MS, resolved.readyPollMs());
    assertEquals(1, resolved.properties().size());

    ResolvedZk zk = resolved.zk();
    assertNotNull(zk);
    assertEquals("zookeeper", zk.service());
    assertEquals(1, zk.requireInstances().size());
    assertEquals(2181, zk.instance().port());
    assertEquals(2181, zk.instance().clientPort());
    assertEquals("zookeeper", zk.instance().host());
    assertEquals("localhost", zk.instance().clientHost());
    assertEquals("zookeeper:2181", zk.clusterHosts());
    assertEquals("localhost:2181", zk.clientHosts());

    ResolvedMetastore ms = resolved.metastore();
    assertNotNull(ms);
    assertEquals("metastore", ms.service());
    assertEquals(1, ms.requireInstances().size());
    assertEquals("jdbc:mysql://localhost:3306/druid", ms.connectURI());
    assertEquals("druid", ms.user());
    assertEquals("diurd", ms.password());

    ResolvedDruidService service = resolved.requireBroker();
    assertNotNull(service);
    assertEquals("broker", service.service());
    assertEquals("http://localhost:8082", service.clientUrl());

    service = resolved.requireRouter();
    assertNotNull(service);
    assertEquals("router", service.service());
    assertEquals("http://localhost:8888", service.clientUrl());
    assertEquals("http://localhost:8888", resolved.routerUrl());
  }
}
