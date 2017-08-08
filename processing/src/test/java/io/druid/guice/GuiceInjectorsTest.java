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

package io.druid.guice;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import org.junit.Assert;
import org.junit.Test;

public class GuiceInjectorsTest
{
  @Test public void testSanity()
  {
    Injector stageOne = GuiceInjectors.makeStartupInjector();

    Module module = stageOne.getInstance(DruidSecondaryModule.class);

    Injector stageTwo = Guice.createInjector(module, new Module()
    {
      @Override public void configure(Binder binder)
      {
        binder.bind(String.class).toInstance("Expected String");
        JsonConfigProvider.bind(binder, "druid.emitter.", Emitter.class);
        binder.bind(CustomEmitter.class).toProvider(new CustomEmitterFactory());
      }
    });


    CustomEmitter customEmitter = stageTwo.getInstance(CustomEmitter.class);

    Assert.assertEquals("Expected String", customEmitter.getOtherValue());
  }

  private static class Emitter
  {

    @JacksonInject
    private String value;

    public String getValue()
    {
      return value;
    }
  }

  private static class CustomEmitterFactory implements Provider<CustomEmitter>
  {

    private Emitter emitter;
    private Injector injector;

    @Inject
    public void configure(Injector injector)
    {
      this.injector = injector;
      emitter = injector.getInstance(Emitter.class);
    }

    @Override public CustomEmitter get()
    {
      return new CustomEmitter(emitter);
    }
  }

  private static class CustomEmitter
  {
    public String getOtherValue()
    {
      return emitter.getValue();
    }

    private Emitter emitter;

    public CustomEmitter(Emitter emitter)
    {
      this.emitter = emitter;
    }
  }
}
