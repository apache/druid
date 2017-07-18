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

package io.druid.storage.cassandra;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.initialization.DruidModule;
import io.druid.segment.loading.DataSegmentPusher;

import java.util.List;

/**
 */
public class CassandraDruidModule implements DruidModule
{
  public static final String SCHEME = "c*";

  @Override
  public void configure(Binder binder)
  {
    Binders.dataSegmentPullerBinder(binder)
           .addBinding(SCHEME)
           .to(CassandraDataSegmentPuller.class)
           .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DataSegmentPusher.class))
            .addBinding(SCHEME)
            .to(CassandraDataSegmentPusher.class)
            .in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.storage", CassandraDataSegmentConfig.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public String getModuleName()
          {
            return "DruidCassandraStorage-" + System.identityHashCode(this);
          }

          @Override
          public Version version()
          {
            return Version.unknownVersion();
          }

          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(CassandraLoadSpec.class);
          }
        }
    );
  }
}
