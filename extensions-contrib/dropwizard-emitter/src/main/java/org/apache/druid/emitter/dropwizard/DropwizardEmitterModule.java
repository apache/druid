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

package org.apache.druid.emitter.dropwizard;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.emitter.core.Emitter;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DropwizardEmitterModule implements DruidModule
{
  private static final String EMITTER_TYPE = "dropwizard";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter." + EMITTER_TYPE, DropwizardEmitterConfig.class);
  }

  @Provides
  @Named(EMITTER_TYPE)
  public Emitter getEmitter(
      DropwizardEmitterConfig dropwizardEmitterConfig,
      ObjectMapper mapper,
      final Injector injector
  )
  {
    List<Emitter> alertEmitters = dropwizardEmitterConfig.getAlertEmitters()
                                                         .stream()
                                                         .map(s -> injector.getInstance(
                                                             Key.get(
                                                                 Emitter.class,
                                                                 Names.named(s)
                                                             )))
                                                         .collect(Collectors.toList());

    return new DropwizardEmitter(dropwizardEmitterConfig, mapper, alertEmitters);
  }
}
