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

package io.druid.client.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import redis.clients.jedis.JedisShardInfo;

import java.util.LinkedList;
import java.util.List;

public class RedisCacheConfig
{

  @JsonProperty
  private List<String> hosts = ImmutableList.of("127.0.0.1:6379");

  @JsonProperty
  private int timeout = 30000;

  @JsonProperty
  private String prefix = "druid";

  @JsonProperty
  private long expiration = -1;

  @JsonProperty
  private int poolSize = 1;

  public int getPoolSize()
  {
    return poolSize;
  }

  public String getPrefix()
  {
    return prefix;
  }

  public long getExpiration()
  {
    return expiration;
  }

  public List<JedisShardInfo> getShardsInfo() {
    List<JedisShardInfo> shardsInfo = new LinkedList<>();
    for (String host : hosts) {
      JedisShardInfo info = new JedisShardInfo(host);
      info.setConnectionTimeout(timeout);
      shardsInfo.add(info);
    }
    return shardsInfo;
  }
}
