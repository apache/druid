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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class MetadataStorageConnectorConfigTest
{

  private MetadataStorageConnectorConfig createMetadataStorageConfig(
      boolean createTables,
      String host,
      int port,
      String connectURI,
      String user,
      String pwdString
  )
      throws IOException
  {
    return JSON_MAPPER.readValue(
        "{" +
        "\"createTables\": \"" + createTables + "\"," +
        "\"host\": \"" + host + "\"," +
        "\"port\": \"" + port + "\"," +
        "\"connectURI\": \"" + connectURI + "\"," +
        "\"user\": \"" + user + "\"," +
        "\"password\": " + pwdString + "," +
        "\"dbcp\": {\n" +
        "  \"maxConnLifetimeMillis\" : 1200000,\n" +
        "  \"defaultQueryTimeout\" : \"30000\"\n" +
        "}" +
        "}",
        MetadataStorageConnectorConfig.class
    );
  }

  @Test
  public void testEquals() throws IOException
  {
    MetadataStorageConnectorConfig metadataStorageConnectorConfig = createMetadataStorageConfig(
        true,
        "testHost",
        4000,
        "url",
        "user",
        "\"nothing\""
    );
    MetadataStorageConnectorConfig metadataStorageConnectorConfig2 = createMetadataStorageConfig(
        true,
        "testHost",
        4000,
        "url",
        "user",
        "\"nothing\""
    );
    Assertions.assertEquals(metadataStorageConnectorConfig, metadataStorageConnectorConfig2);
    Assertions.assertEquals(metadataStorageConnectorConfig.hashCode(), metadataStorageConnectorConfig2.hashCode());
  }

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  @Test
  public void testMetadataStorageConnectionConfigSimplePassword() throws Exception
  {
    testMetadataStorageConnectionConfig(
        true,
        "host",
        1234,
        "connectURI",
        "user",
        "\"nothing\"",
        "nothing"
    );
  }

  @Test
  public void testMetadataStorageConnectionConfigWithDefaultProviderPassword() throws Exception
  {
    testMetadataStorageConnectionConfig(
        true,
        "host",
        1234,
        "connectURI",
        "user",
        "{\"type\":\"default\",\"password\":\"nothing\"}",
        "nothing"
    );
  }

  private void testMetadataStorageConnectionConfig(
      boolean createTables,
      String host,
      int port,
      String connectURI,
      String user,
      String pwdString,
      String pwd
  ) throws Exception
  {
    MetadataStorageConnectorConfig config = JSON_MAPPER.readValue(
        "{" +
        "\"createTables\": \"" + createTables + "\"," +
        "\"host\": \"" + host + "\"," +
        "\"port\": \"" + port + "\"," +
        "\"connectURI\": \"" + connectURI + "\"," +
        "\"user\": \"" + user + "\"," +
        "\"password\": " + pwdString +
        "}",
        MetadataStorageConnectorConfig.class
    );

    Assertions.assertEquals(host, config.getHost());
    Assertions.assertEquals(port, config.getPort());
    Assertions.assertEquals(connectURI, config.getConnectURI());
    Assertions.assertEquals(user, config.getUser());
    Assertions.assertEquals(pwd, config.getPassword());
    Assertions.assertNull(config.getDbcpProperties());
  }

  @Test
  public void testDbcpProperties() throws Exception
  {
    testDbcpPropertiesFile(
        true,
        "host",
        1234,
        "connectURI",
        "user",
        "{\"type\":\"default\",\"password\":\"nothing\"}",
        "nothing"
    );
  }
  private void testDbcpPropertiesFile(
          boolean createTables,
          String host,
          int port,
          String connectURI,
          String user,
          String pwdString,
          String pwd
  ) throws Exception
  {
    MetadataStorageConnectorConfig config = JSON_MAPPER.readValue(
            "{" +
                    "\"createTables\": \"" + createTables + "\"," +
                    "\"host\": \"" + host + "\"," +
                    "\"port\": \"" + port + "\"," +
                    "\"connectURI\": \"" + connectURI + "\"," +
                    "\"user\": \"" + user + "\"," +
                    "\"password\": " + pwdString + "," +
                    "\"dbcp\": {\n" +
                    "  \"maxConnLifetimeMillis\" : 1200000,\n" +
                    "  \"defaultQueryTimeout\" : \"30000\"\n" +
                    "}" +
                    "}",
            MetadataStorageConnectorConfig.class
    );

    Assertions.assertEquals(host, config.getHost());
    Assertions.assertEquals(port, config.getPort());
    Assertions.assertEquals(connectURI, config.getConnectURI());
    Assertions.assertEquals(user, config.getUser());
    Assertions.assertEquals(pwd, config.getPassword());
    Properties dbcpProperties = config.getDbcpProperties();
    Assertions.assertEquals(dbcpProperties.getProperty("maxConnLifetimeMillis"), "1200000");
    Assertions.assertEquals(dbcpProperties.getProperty("defaultQueryTimeout"), "30000");
  }

  @Test
  public void testCreate()
  {
    Map<String, String> props = ImmutableMap.of("key", "value");
    MetadataStorageConnectorConfig config =
        MetadataStorageConnectorConfig.create("connectURI", "user", "pwd", props);
    Assertions.assertEquals("connectURI", config.getConnectURI());
    Assertions.assertEquals("user", config.getUser());
    Assertions.assertEquals("pwd", config.getPassword());
    Assertions.assertEquals(1, config.getDbcpProperties().size());
  }
}
