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

package org.apache.druid.initialization;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.initialization.ServerConfig;
import org.junit.Assert;
import org.junit.Test;

public class ServerConfigSerdeTest
{
  private static final DefaultObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    ServerConfig defaultConfig = new ServerConfig();
    String defaultConfigJson = OBJECT_MAPPER.writeValueAsString(defaultConfig);
    ServerConfig defaultConfig2 = OBJECT_MAPPER.readValue(defaultConfigJson, ServerConfig.class);
    Assert.assertEquals(defaultConfig, defaultConfig2);

    ServerConfig modifiedConfig = new ServerConfig(
        999,
        888,
        defaultConfig.isEnableRequestLimit(),
        defaultConfig.getMaxIdleTime(),
        defaultConfig.getDefaultQueryTimeout(),
        defaultConfig.getMaxScatterGatherBytes(),
        defaultConfig.getMaxQueryTimeout(),
        defaultConfig.getMaxRequestHeaderSize(),
        defaultConfig.getGracefulShutdownTimeout(),
        defaultConfig.getUnannouncePropagationDelay(),
        defaultConfig.getInflateBufferSize(),
        defaultConfig.getCompressionLevel()
    );
    String modifiedConfigJson = OBJECT_MAPPER.writeValueAsString(modifiedConfig);
    ServerConfig modifiedConfig2 = OBJECT_MAPPER.readValue(modifiedConfigJson, ServerConfig.class);
    Assert.assertEquals(modifiedConfig, modifiedConfig2);
    Assert.assertEquals(999, modifiedConfig2.getNumThreads());
    Assert.assertEquals(888, modifiedConfig2.getQueueSize());
  }
}
