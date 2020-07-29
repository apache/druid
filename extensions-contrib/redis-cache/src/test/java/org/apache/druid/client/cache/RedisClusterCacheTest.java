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

package org.apache.druid.client.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class RedisClusterCacheTest
{
  @Test
  public void testConfig() throws JsonProcessingException
  {
    ObjectMapper mapper = new ObjectMapper();
    RedisCacheConfig fromJson = mapper.readValue("{\"expiration\":1000}", RedisCacheConfig.class);
    Assert.assertEquals(1, fromJson.getExpiration().getSeconds());

    fromJson = mapper.readValue("{\"expiration\":\"PT1H\"}", RedisCacheConfig.class);
    Assert.assertEquals(3600, fromJson.getExpiration().getSeconds());
  }

  @Test
  public void test()
  {
    RedisCacheConfig config = new RedisCacheConfig();

    //cluster
  }
}
