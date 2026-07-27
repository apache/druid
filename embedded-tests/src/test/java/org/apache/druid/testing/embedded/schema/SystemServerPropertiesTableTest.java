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

public class SystemServerPropertiesTableTest extends EmbeddedClusterTestBase
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
      .addProperty("test.onlyBroker", "brokerValue")
      .addProperty("test.nonUniqueProperty", "brokerNonUniqueValue")
      .addProperty("password", "brokerPassword");

  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.service", OVERLORD_SERVICE)
      .addProperty("druid.plaintextPort", OVERLORD_PORT)
      .addProperty("test.onlyOverlord", "overlordValue")
      .addProperty("test.nonUniqueProperty", "overlordNonUniqueValue");

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.service", COORDINATOR_SERVICE)
      .addProperty("druid.plaintextPort", COORDINATOR_PORT)
      .addProperty("test.onlyCoordinator", "coordinatorValue")
      .addProperty("test.nonUniqueProperty", "coordinatorNonUniqueValue");

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
  public void test_serverPropertiesTable_brokerServer()
  {
    final Map<String, String> brokerProps = cluster.callApi().serviceClient().onAnyBroker(
        mapper -> new RequestBuilder(HttpMethod.GET, "/status/properties"),
        new TypeReference<>(){}
    );
    verifyPropertiesForServer(brokerProps, BROKER_SERVICE, StringUtils.format("localhost:%s", BROKER_PORT), NodeRole.BROKER_JSON_NAME);
  }

  @Test
  public void test_serverPropertiesTable_overlordServer()
  {
    final Map<String, String> overlordProps = cluster.callApi().serviceClient().onLeaderOverlord(
        mapper -> new RequestBuilder(HttpMethod.GET, "/status/properties"),
        new TypeReference<>(){}
    );
    verifyPropertiesForServer(overlordProps, OVERLORD_SERVICE, StringUtils.format("localhost:%s", OVERLORD_PORT), NodeRole.OVERLORD_JSON_NAME);
  }

  @Test
  public void test_serverPropertiesTable_coordinatorServer()
  {
    final Map<String, String> coordinatorProps = cluster.callApi().serviceClient().onLeaderCoordinator(
        mapper -> new RequestBuilder(HttpMethod.GET, "/status/properties"),
        new TypeReference<>(){}
    );
    verifyPropertiesForServer(coordinatorProps, COORDINATOR_SERVICE, StringUtils.format("localhost:%s", COORDINATOR_PORT), NodeRole.COORDINATOR_JSON_NAME);
  }

  @Test
  public void test_serverPropertiesTable_specificProperty()
  {
    Assertions.assertEquals(
        "brokerValue",
        cluster.runSql("SELECT \"value\" FROM sys.server_properties WHERE server = 'localhost:%s' AND property = 'test.onlyBroker'", BROKER_PORT)
    );

    Assertions.assertEquals(
        "brokerValue",
        cluster.runSql("SELECT \"value\" FROM sys.server_properties WHERE service_name = '%s' AND property = 'test.onlyBroker'", BROKER_SERVICE)
    );

    Assertions.assertEquals(
        StringUtils.format("localhost:%s,%s,[%s],test.onlyBroker,brokerValue,", BROKER_PORT, BROKER_SERVICE, NodeRole.BROKER_JSON_NAME),
        cluster.runSql("SELECT * FROM sys.server_properties WHERE server = 'localhost:%s' AND property = 'test.onlyBroker'", BROKER_PORT)
    );

    String[] expectedRows = new String[] {
        StringUtils.format("localhost:%s,%s,[%s],test.nonUniqueProperty,brokerNonUniqueValue,", BROKER_PORT, BROKER_SERVICE, NodeRole.BROKER_JSON_NAME),
        StringUtils.format("localhost:%s,%s,[%s],test.nonUniqueProperty,overlordNonUniqueValue,", OVERLORD_PORT, OVERLORD_SERVICE, NodeRole.OVERLORD_JSON_NAME),
        StringUtils.format("localhost:%s,%s,[%s],test.nonUniqueProperty,coordinatorNonUniqueValue,", COORDINATOR_PORT, COORDINATOR_SERVICE, NodeRole.COORDINATOR_JSON_NAME),
    };
    Arrays.sort(expectedRows, String::compareTo);
    final String result = cluster.runSql("SELECT * FROM sys.server_properties WHERE property='test.nonUniqueProperty'");
    String[] actualRows = result.split("\n");
    Arrays.sort(actualRows, String::compareTo);
    Assertions.assertArrayEquals(expectedRows, actualRows);
        
  }

  @Test
  public void test_serverPropertiesTable_hiddenProperties()
  {
    final Map<String, String> brokerProps = cluster.callApi().serviceClient().onAnyBroker(
        mapper -> new RequestBuilder(HttpMethod.GET, "/status/properties"),
        new TypeReference<>(){}
    );
    Assertions.assertFalse(brokerProps.containsKey("password"));
  }

  @Test
  public void test_serverPropertiesTable_serverFilterPushdown()
  {
    final String brokerHost = StringUtils.format("localhost:%s", BROKER_PORT);

    // Equality filter returns only matching server rows
    final String result = cluster.runSql(
        "SELECT server, service_name, property FROM sys.server_properties WHERE server = '%s'",
        brokerHost
    );
    Assertions.assertFalse(result.isEmpty(), "Should return properties for the broker");
    for (String row : result.split("\n")) {
      Assertions.assertTrue(
          row.startsWith(brokerHost + ","),
          "Row should belong to filtered server: " + row
      );
    }

    // Non-existent server returns no rows
    final String emptyResult = cluster.runSql(
        "SELECT * FROM sys.server_properties WHERE server = 'nonexistent:9999'"
    );
    Assertions.assertTrue(emptyResult.isEmpty(), "Non-existent server filter should return no rows");

    // != is not consumed — falls back to Calcite post-filter, still correct
    final String neResult = cluster.runSql(
        "SELECT DISTINCT server FROM sys.server_properties WHERE server != '%s'",
        brokerHost
    );
    Assertions.assertFalse(neResult.isEmpty(), "!= filter should still return other servers");
    for (String row : neResult.split("\n")) {
      Assertions.assertFalse(
          row.trim().equals(brokerHost),
          "!= filter should exclude the broker: " + row
      );
    }

    // AND with a non-pushdown predicate — server filter consumed, rest handled by Calcite
    final String andResult = cluster.runSql(
        "SELECT server, property FROM sys.server_properties WHERE server = '%s' AND node_roles LIKE '%%broker%%'",
        brokerHost
    );
    Assertions.assertFalse(andResult.isEmpty(), "AND with node_roles filter should return rows");
    for (String row : andResult.split("\n")) {
      Assertions.assertTrue(
          row.startsWith(brokerHost + ","),
          "Row should belong to filtered server: " + row
      );
    }
  }

  @Test
  public void test_serverPropertiesTable_serviceNameFilterPushdown()
  {
    final String brokerHost = StringUtils.format("localhost:%s", BROKER_PORT);

    // Equality filter on service_name returns only matching rows
    final String result = cluster.runSql(
        "SELECT server, service_name, property FROM sys.server_properties WHERE service_name = '%s'",
        BROKER_SERVICE
    );
    Assertions.assertFalse(result.isEmpty(), "Should return properties for the broker service");
    for (String row : result.split("\n")) {
      String[] cols = row.split(",", -1);
      Assertions.assertEquals(BROKER_SERVICE, cols[1], "Row should belong to filtered service_name: " + row);
    }

    // Non-existent service_name returns no rows
    final String emptyResult = cluster.runSql(
        "SELECT * FROM sys.server_properties WHERE service_name = 'nonexistent/service'"
    );
    Assertions.assertTrue(emptyResult.isEmpty(), "Non-existent service_name filter should return no rows");

    // != falls back to Calcite post-filter
    final String neResult = cluster.runSql(
        "SELECT DISTINCT service_name FROM sys.server_properties WHERE service_name != '%s'",
        BROKER_SERVICE
    );
    Assertions.assertFalse(neResult.isEmpty(), "!= filter should still return other services");
    for (String row : neResult.split("\n")) {
      Assertions.assertFalse(
          row.trim().equals(BROKER_SERVICE),
          "!= filter should exclude the broker service: " + row
      );
    }

    // Both server and service_name filters consumed together
    final String andResult = cluster.runSql(
        "SELECT server, service_name, property FROM sys.server_properties WHERE service_name = '%s' AND server = '%s'",
        BROKER_SERVICE, brokerHost
    );
    Assertions.assertFalse(andResult.isEmpty(), "AND with server and service_name should return rows");
    for (String row : andResult.split("\n")) {
      String[] cols = row.split(",", -1);
      Assertions.assertEquals(brokerHost, cols[0], "Row server should match: " + row);
      Assertions.assertEquals(BROKER_SERVICE, cols[1], "Row service_name should match: " + row);
    }
  }

  @Test
  public void test_serverPropertiesTable_errorMessageIsNullForHealthyServers()
  {
    // All 3 servers in the embedded cluster are healthy, so no rows should have a non-null error_message
    final String errorRows = cluster.runSql("SELECT server FROM sys.server_properties WHERE error_message IS NOT NULL");
    Assertions.assertTrue(errorRows.isEmpty(), "Healthy servers should have null error_message");

    // Every row should have a null error_message
    final String totalCount = cluster.runSql("SELECT COUNT(*) FROM sys.server_properties");
    final String nullErrorCount = cluster.runSql("SELECT COUNT(*) FROM sys.server_properties WHERE error_message IS NULL");
    Assertions.assertEquals(totalCount, nullErrorCount, "All rows should have null error_message in a healthy cluster");
  }

  private void verifyPropertiesForServer(Map<String, String> properties, String serivceName, String hostAndPort, String nodeRole)
  {
    String[] expectedRows = properties.entrySet().stream().map(entry -> String.join(
        ",",
        escapeCsvField(hostAndPort),
        escapeCsvField(serivceName),
        escapeCsvField(ImmutableList.of(nodeRole).toString()),
        escapeCsvField(entry.getKey()),
        escapeCsvField(entry.getValue()),
        escapeCsvField(null)
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
