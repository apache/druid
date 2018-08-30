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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Preconditions;
import com.google.inject.name.Named;

public class HybridCacheProvider extends HybridCacheConfig implements CacheProvider
{
  final CacheProvider level1;
  final CacheProvider level2;

  @JsonCreator
  public HybridCacheProvider(
      @JacksonInject @Named("l1") CacheProvider level1,
      @JacksonInject @Named("l2") CacheProvider level2
  )
  {
    this.level1 = Preconditions.checkNotNull(level1, "l1 cache not specified for hybrid cache");
    this.level2 = Preconditions.checkNotNull(level2, "l2 cache not specified for hybrid cache");
    if (!getUseL2() && !getPopulateL2()) {
      throw new IllegalStateException(
          "Doesn't make sense to use Hybrid cache with both use and populate disabled for L2, "
          + "use just L1 cache in this case"
      );
    }
  }

  @Override
  public Cache get()
  {
    return new HybridCache(this, level1.get(), level2.get());
  }
}
