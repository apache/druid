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

package org.apache.druid.initialization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.server.emitter.SwitchingEmitterConfig;
import org.apache.druid.server.emitter.SwitchingEmitterModule;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class SwitchingEmitterModuleTest
{

  private static final String DEFAULT_EMITTER_TYPE = "http";
  private static final String FEED_1_EMITTER_TYPE = "logging";
  private static final String FEED_1 = "metrics";
  private Emitter defaultEmitter;
  private Emitter feed1Emitter;

  @Before
  public void setup()
  {
    defaultEmitter = EasyMock.createMock(Emitter.class);
    feed1Emitter = EasyMock.createMock(Emitter.class);
    defaultEmitter.start();
    feed1Emitter.start();
    EasyMock.replay(defaultEmitter);
    EasyMock.replay(feed1Emitter);
  }

  @Test
  public void testGetEmitter()
  {
    SwitchingEmitterConfig config = EasyMock.createMock(SwitchingEmitterConfig.class);
    EasyMock.expect(config.getDefaultEmitter()).andReturn(ImmutableList.of(DEFAULT_EMITTER_TYPE)).anyTimes();
    EasyMock.expect(config.getEmitters()).andReturn(ImmutableMap.of(FEED_1, ImmutableList.of(FEED_1_EMITTER_TYPE))).anyTimes();

    Injector injector = EasyMock.createMock(Injector.class);
    EasyMock.expect(injector.getInstance(Key.get(Emitter.class, Names.named(DEFAULT_EMITTER_TYPE)))).andReturn(
        defaultEmitter);
    EasyMock.expect(injector.getInstance(Key.get(Emitter.class, Names.named(FEED_1_EMITTER_TYPE)))).andReturn(
        feed1Emitter);
    EasyMock.replay(config, injector);

    Emitter switchingEmitter = new SwitchingEmitterModule().makeEmitter(config, injector);
    switchingEmitter.start();

    EasyMock.verify(config, defaultEmitter, feed1Emitter, injector);
  }

  @Test
  public void testGetEmitterViaRealGuice()
  {
    Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new JacksonModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            Properties props = new Properties();
            String defaultEmittersValue = "[\"" + DEFAULT_EMITTER_TYPE + "\"]";
            String emittersValue = "{\"" + FEED_1 + "\":[\"" + FEED_1_EMITTER_TYPE + "\"]}";
            props.put("druid.emitter.switching.defaultEmitters", defaultEmittersValue);
            props.put("druid.emitter.switching.emitters", emittersValue);
            binder.bind(Properties.class).toInstance(props);
            binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
            binder.bind(Emitter.class).annotatedWith(Names.named(DEFAULT_EMITTER_TYPE)).toInstance(defaultEmitter);
            binder.bind(Emitter.class).annotatedWith(Names.named(FEED_1_EMITTER_TYPE)).toInstance(feed1Emitter);
          }
        },
        new SwitchingEmitterModule()
    );
    injector.getInstance(Key.get(Emitter.class, Names.named("switching"))).start();
    EasyMock.verify(defaultEmitter);
    EasyMock.verify(feed1Emitter);

  }
}
