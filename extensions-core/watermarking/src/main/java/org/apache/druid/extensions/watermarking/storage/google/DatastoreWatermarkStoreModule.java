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

package org.apache.druid.extensions.watermarking.storage.google;


import com.fasterxml.jackson.databind.Module;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.datastore.v1.Datastore;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.extensions.watermarking.storage.WatermarkStoreModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.server.DruidNode;

import java.util.List;

public class DatastoreWatermarkStoreModule extends WatermarkStoreModule implements DruidModule
{
  public static final String TYPE = "datastore";

  public DatastoreWatermarkStoreModule()
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
        STORE_PROPERTY_BASE + TYPE,
        DatastoreWatermarkStoreConfig.class
    );

    PolyBind.optionBinder(binder, Key.get(WatermarkSink.class))
            .addBinding(TYPE)
            .to(DatastoreWatermarkStore.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(WatermarkSource.class))
            .addBinding(TYPE)
            .to(DatastoreWatermarkStore.class)
            .in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public Datastore getDatastore(
      HttpTransport httpTransport,
      HttpRequestInitializer httpRequestInitializer,
      JsonFactory jsonFactory,
      @Self DruidNode me
  )
  {
    return new Datastore.Builder(
        httpTransport,
        jsonFactory,
        httpRequestInitializer
    ).setApplicationName(
        me.getServiceName()
    ).build();
  }
}
