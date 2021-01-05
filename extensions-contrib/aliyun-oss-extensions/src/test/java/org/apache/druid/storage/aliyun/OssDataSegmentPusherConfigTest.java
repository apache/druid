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

package org.apache.druid.storage.aliyun;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class OssDataSegmentPusherConfigTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerialization() throws IOException
  {
    String jsonConfig = "{\"bucket\":\"bucket1\",\"prefix\":\"dataSource1\"}";

    OssStorageConfig config = JSON_MAPPER.readValue(jsonConfig, OssStorageConfig.class);
    Assert.assertEquals(jsonConfig, JSON_MAPPER.writeValueAsString(config));
  }

  @Test
  public void testSerializationWithDefaults() throws IOException
  {
    String jsonConfig = "{\"bucket\":\"bucket1\",\"prefix\":\"dataSource1\"}";
    String expectedJsonConfig = "{\"bucket\":\"bucket1\",\"prefix\":\"dataSource1\"}";

    OssStorageConfig config = JSON_MAPPER.readValue(jsonConfig, OssStorageConfig.class);
    Assert.assertEquals(expectedJsonConfig, JSON_MAPPER.writeValueAsString(config));
  }
}
