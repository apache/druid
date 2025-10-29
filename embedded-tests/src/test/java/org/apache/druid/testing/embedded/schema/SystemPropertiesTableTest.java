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
  private static final String HOST = "localhost";
  private static final String BROKER_PORT = "8082";
  private static final String OVERLORD_PORT = "8090";
  private static final String BROKER_SERVICE = "test/broker";
  private static final String OVERLORD_SERVICE = "test/overlord";

  private final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("test.onlyBroker", "brokerValue")
      .addProperty("mytestbrokerproperty", "mytestbrokervalue")
      .addProperty("druid.host", HOST)
      .addProperty("druid.plaintextPort", BROKER_PORT)
      .addProperty("druid.service", BROKER_SERVICE);

  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.service", OVERLORD_SERVICE)
      .addProperty("druid.host", HOST)
      .addProperty("druid.plaintextPort", OVERLORD_PORT)
      .addProperty("test.onlyOverlord", "overlordValue");


  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withZookeeper()
        .addServer(new EmbeddedCoordinator())
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
    verifyPropertiesForServer(overlordProps, OVERLORD_SERVICE, getHostAndPort(HOST, OVERLORD_PORT), NodeRole.OVERLORD_JSON_NAME);

    final Map<String, String> brokerProps = cluster.callApi().serviceClient().onAnyBroker(
        mapper -> new RequestBuilder(HttpMethod.GET, "/status/properties"),
        new TypeReference<>(){}
    );
    verifyPropertiesForServer(brokerProps, BROKER_SERVICE, getHostAndPort(HOST, BROKER_PORT), NodeRole.BROKER_JSON_NAME);
    final String test = cluster.runSql("SELECT * FROM sys.server_properties");
  }

  private void verifyPropertiesForServer(Map<String, String> properties, String serivceName, String hostAndPort, String nodeRole)
  {
    String[] expectedRows = properties.entrySet().stream().map(entry -> String.join(
        ",",
        escapeCsvField(serivceName),
        escapeCsvField(hostAndPort),
        escapeCsvField(nodeRole),
        escapeCsvField(entry.getKey()),
        escapeCsvField(entry.getValue())
    )).toArray(String[]::new);
    Arrays.sort(expectedRows, String::compareTo);
    String[] actualRows = Arrays.stream(cluster.runSql("SELECT * FROM sys.server_properties WHERE server='%s'", hostAndPort).split("\n")).map(entry -> StringUtils.replace(entry, "...", "\"\"")).toArray(String[]::new);
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

  private String getHostAndPort(String host, String port)
  {
    return StringUtils.format("%s:%s", host, port);
  }
}
