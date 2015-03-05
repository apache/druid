/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.metadata;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MetadataStorageConnectorConfigTest {

  private static final ObjectMapper jsonMapper = new ObjectMapper();

  @Test
  public void testMetadaStorageConnectionConfigSimplePassword() throws Exception
  {
    testMetadataStorageConnectionConfig(
        true,
        "host",
        1234,
        "connectURI",
        "user",
        "\"nothing\"",
        "nothing");
  }

  @Test
  public void testMetadaStorageConnectionConfigWithDefaultProviderPassword() throws Exception
  {
    testMetadataStorageConnectionConfig(
        true,
        "host",
        1234,
        "connectURI",
        "user",
        "{\"type\":\"default\",\"password\":\"nothing\"}",
        "nothing");
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
    MetadataStorageConnectorConfig config = jsonMapper.readValue(
          "{" +
          "\"createTables\": \"" + createTables + "\"," +
          "\"host\": \"" + host + "\"," +
          "\"port\": \"" + port + "\"," +
          "\"connectURI\": \"" + connectURI + "\"," +
          "\"user\": \"" + user + "\"," +
          "\"password\": " + pwdString +
          "}",
          MetadataStorageConnectorConfig.class);

    Assert.assertEquals(host, config.getHost());
    Assert.assertEquals(port, config.getPort());
    Assert.assertEquals(connectURI, config.getConnectURI());
    Assert.assertEquals(user, config.getUser());
    Assert.assertEquals(pwd, config.getPassword());
  }
}
