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

package io.druid.initialization;

import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.emitter.core.Emitter;
import io.druid.guice.DruidGuiceExtensions;
import io.druid.guice.LifecycleModule;
import io.druid.server.initialization.ComposingEmitterConfig;
import io.druid.server.initialization.ComposingEmitterModule;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

/**
 */
public class ComposingEmitterModuleTest
{
  private final String testEmitterType = "http";
  private Emitter emitter;

  @Before
  public void setup()
  {
    emitter = EasyMock.createMock(Emitter.class);
    emitter.start();
    EasyMock.replay(emitter);
  }

  @Test
  public void testGetEmitter()
  {
    ComposingEmitterConfig config = EasyMock.createMock(ComposingEmitterConfig.class);
    EasyMock.expect(config.getEmitters()).andReturn(Lists.newArrayList(testEmitterType)).anyTimes();

    Injector injector = EasyMock.createMock(Injector.class);
    EasyMock.expect(injector.getInstance(Key.get(Emitter.class, Names.named(testEmitterType)))).andReturn(emitter);
    EasyMock.replay(config, injector);

    Emitter composingEmitter = new ComposingEmitterModule().getEmitter(config, injector);
    composingEmitter.start();

    EasyMock.verify(config, emitter, injector);
  }

  @Test
  public void testGetEmitterViaRealGuice()
  {
    Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            Properties props = new Properties();
            props.put("druid.emitter.composing.emitters", "[\"" + testEmitterType + "\"]");
            binder.bind(Properties.class).toInstance(props);
            binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
            binder.bind(Emitter.class).annotatedWith(Names.named(testEmitterType)).toInstance(emitter);
          }
        },
        new ComposingEmitterModule()
    );
    injector.getInstance(Key.get(Emitter.class, Names.named("composing"))).start();
    EasyMock.verify(emitter);
  }
}
