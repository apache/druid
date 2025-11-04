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

package org.apache.druid.testing.embedded.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

public class SystemPropertiesTableTest extends EmbeddedClusterTestBase
{
  private static final String BROKER_PORT = "9082";
  private static final String BROKER_SERVICE = "test/broker";
  private static final String OVERLORD_PORT = "9090";
  private static final String OVERLORD_SERVICE = "test/overlord";
  private static final String COORDINATOR_PORT = "9081";
  private static final String COORDINATOR_SERVICE = "test/coordinator";

  private final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.service", BROKER_SERVICE)
      .addProperty("druid.plaintextPort", BROKER_PORT)
      .addProperty("test.onlyBroker", "brokerValue");

  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
       .addProperty("druid.service", OVERLORD_SERVICE)
       .addProperty("druid.plaintextPort", OVERLORD_PORT)
       .addProperty("test.onlyOverlord", "overlordValue");

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.service", COORDINATOR_SERVICE)
      .addProperty("druid.plaintextPort", COORDINATOR_PORT)
      .addProperty("test.onlyCoordinator", "coordinatorValue");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withZookeeper()
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(broker)
        .addCommonProperty("commonProperty", "commonValue");
  }

  @Test
  public void test_serverPropertiesTable()
  {
    final Map<String, String> overlordProps = cluster.callApi().serviceClient().onLeaderOverlord(
        mapper -> new RequestBuilder(HttpMethod.GET, "/status/properties"),
        new TypeReference<>(){}
    );
    verifyPropertiesForServer(overlordProps, OVERLORD_SERVICE, StringUtils.format("localhost:%s", OVERLORD_PORT), NodeRole.OVERLORD_JSON_NAME);

    final Map<String, String> brokerProps = cluster.callApi().serviceClient().onAnyBroker(
        mapper -> new RequestBuilder(HttpMethod.GET, "/status/properties"),
        new TypeReference<>(){}
    );
    verifyPropertiesForServer(brokerProps, BROKER_SERVICE, StringUtils.format("localhost:%s", BROKER_PORT), NodeRole.BROKER_JSON_NAME);

    final Map<String, String> coordinatorProps = cluster.callApi().serviceClient().onLeaderCoordinator(
        mapper -> new RequestBuilder(HttpMethod.GET, "/status/properties"),
        new TypeReference<>(){}
    );
    verifyPropertiesForServer(coordinatorProps, COORDINATOR_SERVICE, StringUtils.format("localhost:%s", COORDINATOR_PORT), NodeRole.COORDINATOR_JSON_NAME);
  }

  private void verifyPropertiesForServer(Map<String, String> properties, String serivceName, String hostAndPort, String nodeRole)
  {
    String[] expectedRows = properties.entrySet().stream().map(entry -> String.join(
        ",",
        escapeCsvField(hostAndPort),
        escapeCsvField(serivceName),
        escapeCsvField(ImmutableList.of(nodeRole).toString()),
        escapeCsvField(entry.getKey()),
        escapeCsvField(entry.getValue())
    )).toArray(String[]::new);
    Arrays.sort(expectedRows, String::compareTo);
    final String result = cluster.runSql("SELECT * FROM sys.server_properties WHERE server='%s'", hostAndPort);
    String[] actualRows = result.split("\n");
    Arrays.sort(actualRows, String::compareTo);
    Assertions.assertArrayEquals(expectedRows, actualRows);
  }

  /**
   * Escapes a field value for CSV format.
   */
  private String escapeCsvField(String field)
  {
    if (field == null) {
      return "";
    }
    if (field.contains(",") || field.contains("\"") || field.contains("\n") || field.contains("\r")) {
      return "\"" + StringUtils.replace(field, "\"", "\"\"") + "\"";
    }
    return field;
  }
}
