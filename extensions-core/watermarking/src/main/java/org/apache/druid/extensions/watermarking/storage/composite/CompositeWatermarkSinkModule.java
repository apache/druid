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

package org.apache.druid.extensions.watermarking.storage.composite;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.WatermarkSinkModule;
import org.apache.druid.extensions.watermarking.storage.WatermarkStoreModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.ArrayList;
import java.util.List;

public class CompositeWatermarkSinkModule extends WatermarkSinkModule implements DruidModule
{
  public static final String TYPE = "composite";
  private static final Logger LOG = new Logger(CompositeWatermarkSinkModule.class);

  public CompositeWatermarkSinkModule()
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

    JsonConfigProvider.bind(
        binder,
        WatermarkStoreModule.STORE_PROPERTY_BASE + TYPE,
        CompositeWatermarkSinkConfig.class
    );

    PolyBind.optionBinder(binder, Key.get(WatermarkSink.class))
            .addBinding(TYPE)
            .to(CompositeWatermarkSink.class)
            .in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public List<WatermarkSink> getWatermarkSinks(
      CompositeWatermarkSinkConfig compositeConfig,
      Injector injector
  ) throws Exception
  {
    List<WatermarkSink> watermarkSinks = new ArrayList<>();

    final ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try {
      // gRPC is weird about class loading
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      for (String sinkClass : compositeConfig.getSinks()) {
        final WatermarkSink sink = (WatermarkSink) injector.getInstance(Class.forName(sinkClass));

        LOG.debug("Adding WatermarkSink [%s] to CompositeWatermarkSink", sink);

        watermarkSinks.add(sink);
      }

      return watermarkSinks;
    }
    finally {
      Thread.currentThread().setContextClassLoader(loader);
    }
  }
}
