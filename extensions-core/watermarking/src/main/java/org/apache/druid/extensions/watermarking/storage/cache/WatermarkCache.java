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

package org.apache.druid.extensions.watermarking.storage.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.java.util.common.Pair;
import org.joda.time.DateTime;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

public class WatermarkCache
{
  private final Cache<Pair<String, String>, DateTime> cache;
  private final WatermarkSource source;
  private final WatermarkCacheConfig config;

  @Inject
  public WatermarkCache(
      WatermarkSource source,
      WatermarkCacheConfig config
  )
  {
    this.config = config;
    this.source = source;
    this.cache = Caffeine.newBuilder()
                         .expireAfterWrite(config.getExpireMinutes(), TimeUnit.MINUTES)
                         .maximumSize(config.getMaxSize())
                         .build();
  }

  public void set(String datasource, String type, DateTime value)
  {
    if (config.getEnable()) {
      this.cache.put(new Pair<>(datasource, type), value);
    }
  }

  public DateTime get(String datasource, String type)
  {
    if (config.getEnable()) {
      return this.cache.get(
          new Pair<>(datasource, type),
          key -> source.getValue(key.lhs, key.rhs)
      );
    }
    return source.getValue(datasource, type);
  }

  public WatermarkCacheConfig getConfig()
  {
    return config;
  }

  public WatermarkSource getSource()
  {
    return source;
  }
}
