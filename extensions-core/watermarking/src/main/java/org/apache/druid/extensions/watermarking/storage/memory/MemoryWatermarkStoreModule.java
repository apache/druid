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

package org.apache.druid.extensions.watermarking.storage.memory;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.extensions.watermarking.storage.WatermarkStoreModule;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryWatermarkStoreModule extends WatermarkStoreModule implements DruidModule
{
  public static final String TYPE = "memory";

  public MemoryWatermarkStoreModule()
  {
    super(TYPE);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    super.configure(binder);

    PolyBind.createChoiceWithDefault(
        binder,
        SOURCE_PROPERTY,
        Key.get(WatermarkSource.class),
        TYPE
    );
    PolyBind.createChoiceWithDefault(
        binder,
        SINK_PROPERTY,
        Key.get(WatermarkSink.class),
        TYPE
    );

    PolyBind.optionBinder(binder, Key.get(WatermarkSink.class))
            .addBinding(TYPE)
            .to(MemoryWatermarkStore.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(WatermarkSource.class))
            .addBinding(TYPE)
            .to(MemoryWatermarkStore.class)
            .in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public ConcurrentHashMap<String, Map<String, MemoryWatermarkStore.MemoryWatermarkState>> getStore()
  {
    return new ConcurrentHashMap<>();
  }
}
