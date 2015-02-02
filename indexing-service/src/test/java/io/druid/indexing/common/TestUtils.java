/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.metamx.common.ISE;
import io.druid.guice.ServerModule;
import io.druid.jackson.DefaultObjectMapper;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 */
public class TestUtils
{
  public static final ObjectMapper MAPPER = new DefaultObjectMapper();

  static {
    final List<? extends Module> list = new ServerModule().getJacksonModules();
    for (Module module : list) {
      MAPPER.registerModule(module);
    }
    MAPPER.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
          )
          {
            if (valueId.equals("com.fasterxml.jackson.databind.ObjectMapper")) {
              return TestUtils.MAPPER;
            }
            throw new ISE("No Injectable value found");
          }
        }
    );
  }

  public static boolean conditionValid(IndexingServiceCondition condition)
  {
    try {
      Stopwatch stopwatch = Stopwatch.createUnstarted();
      stopwatch.start();
      while (!condition.isValid()) {
        Thread.sleep(100);
        if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > 1000) {
          throw new ISE("Cannot find running task");
        }
      }
    }
    catch (Exception e) {
      return false;
    }
    return true;
  }
}
