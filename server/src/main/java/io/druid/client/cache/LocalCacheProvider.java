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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.query.DruidProcessingConfig;

import javax.validation.constraints.Min;
import java.util.Map;

/**
 */
public class LocalCacheProvider implements CacheProvider
{
  public static final NoopCache NOOP_CACHE = new NoopCache();

  @JsonProperty
  @Min(0)
  private long sizeInBytes = 0;

  @JsonProperty
  @Min(0)
  private int initialSize = 500000;

  @JacksonInject
  private DruidProcessingConfig config = null;

  @Override
  public Cache get()
  {
    if(sizeInBytes > 0) {
      return MapCache.create(sizeInBytes, initialSize, config.getNumThreads());
    } else {
      return NOOP_CACHE;
    }
  }

  private static class NoopCache implements Cache
  {
    @Override
    public byte[] get(NamedKey key)
    {
      return null;
    }

    @Override
    public void put(NamedKey key, byte[] value)
    {
    }

    @Override
    public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
    {
      return ImmutableMap.of();
    }

    @Override
    public void close(String namespace)
    {

    }

    @Override
    public CacheStats getStats()
    {
      return new CacheStats(0,0,0,0,0,0,0);
    }

    @Override
    public boolean isLocal()
    {
      return true;
    }

    @Override
    public void doMonitor(ServiceEmitter emitter)
    {

    }
  }
}
