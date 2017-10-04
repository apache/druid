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

package io.druid.emitter.graphite;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.metamx.emitter.core.Emitter;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

public class GraphiteEmitterModule implements DruidModule
{
  private static final String EMITTER_TYPE = "graphite";
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter." + EMITTER_TYPE, GraphiteEmitterConfig.class);
  }

  @Provides
  @ManageLifecycle
  @Named(EMITTER_TYPE)
  public Emitter getEmitter(GraphiteEmitterConfig graphiteEmitterConfig, ObjectMapper mapper, final Injector injector)
  {
    List<Emitter> emitters = ImmutableList.copyOf(
        Lists.transform(
            graphiteEmitterConfig.getAlertEmitters(),
            alertEmitterName -> {
              return injector.getInstance(Key.get(Emitter.class, Names.named(alertEmitterName)));
            }
        )
    );

    List<Emitter> requestLogEmitters = ImmutableList.copyOf(
        Lists.transform(
            graphiteEmitterConfig.getRequestLogEmitters(),
            requestLogEmitterName -> {
              return injector.getInstance(Key.get(Emitter.class, Names.named(requestLogEmitterName)));
            }
        )
    );
    return new GraphiteEmitter(graphiteEmitterConfig, emitters, requestLogEmitters);
  }
}
