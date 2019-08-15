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

package org.apache.druid.emitter.influxdb;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;

import java.util.Collections;
import java.util.List;

public class InfluxdbEmitterModule implements DruidModule
{

  private static final String EMITTER_TYPE = "influxdb";
  private static final Logger log = new Logger(InfluxdbEmitterModule.class);

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter." + EMITTER_TYPE, InfluxdbEmitterConfig.class);
  }

  @Provides
  @ManageLifecycle
  @Named(EMITTER_TYPE)
  public Emitter getEmitter(InfluxdbEmitterConfig influxdbEmitterConfig, ObjectMapper mapper)
  {
    return new InfluxdbEmitter(influxdbEmitterConfig);
  }
}
