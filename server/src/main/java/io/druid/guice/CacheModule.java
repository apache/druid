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

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheProvider;
import io.druid.guice.annotations.Global;

public class CacheModule implements Module
{

  public static final String DRUID_GLOBAL_CACHE_PREFIX = "druid.cache";

  public final String prefix;

  public CacheModule()
  {
    this.prefix = DRUID_GLOBAL_CACHE_PREFIX;
  }

  public CacheModule(String prefix)
  {
    this.prefix = prefix;
  }

  @Override
  public void configure(Binder binder)
  {
    binder.bind(Cache.class).toProvider(Key.get(CacheProvider.class, Global.class)).in(ManageLifecycle.class);
    JsonConfigProvider.bind(binder, prefix, CacheProvider.class, Global.class);

    binder.install(new HybridCacheModule(prefix));
  }

  public static class HybridCacheModule implements Module
  {

    private final String prefix;

    public HybridCacheModule(String prefix)
    {
      this.prefix = prefix;
    }

    @Override
    public void configure(Binder binder)
    {
      JsonConfigProvider.bind(binder, prefix + ".l1", CacheProvider.class, Names.named("l1"));
      JsonConfigProvider.bind(binder, prefix + ".l2", CacheProvider.class, Names.named("l2"));
    }
  }
}
