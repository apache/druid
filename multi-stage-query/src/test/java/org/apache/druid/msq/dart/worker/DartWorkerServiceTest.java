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

package org.apache.druid.msq.dart.worker;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.ServerModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.server.DruidNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies a Historical's {@link DartWorkerService} announcement round-trips through the {@link DruidService}
 * polymorphic types, so any process with {@link MSQIndexingModule} installed can read it and discover the node.
 */
public class DartWorkerServiceTest
{
  private static final DruidNode HISTORICAL_NODE =
      new DruidNode("no", "localhost", false, 8083, -1, true, false);

  @Test
  public void test_serde_withinDiscoveryDruidNode() throws Exception
  {
    final ObjectMapper mapper = createMapper();
    final DiscoveryDruidNode node = new DiscoveryDruidNode(
        HISTORICAL_NODE,
        NodeRole.HISTORICAL,
        ImmutableMap.of(DartWorkerService.NAME, new DartWorkerService())
    );
    final String json = mapper.writeValueAsString(node);
    Assertions.assertEquals(node, mapper.readValue(json, DiscoveryDruidNode.class));
  }

  private static ObjectMapper createMapper()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerModules(new ServerModule().getJacksonModules());
    mapper.registerModules(new MSQIndexingModule().getJacksonModules());
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, mapper));
    return mapper;
  }
}
