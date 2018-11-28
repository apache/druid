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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.guice.GuiceAnnotationIntrospector;
import org.apache.druid.guice.GuiceInjectableValues;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

/**
 *
 */
@RunWith(Parameterized.class)
public class LoadSpecTest
{
  @Parameterized.Parameters
  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.of(
        new Object[]{"{\"path\":\"/\",\"type\":\"local\"}", "local"}
    );
  }

  private final String value;
  private final String expectedId;

  public LoadSpecTest(String value, String expectedId)
  {
    this.value = value;
    this.expectedId = expectedId;
  }

  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUp()
  {
    final Injector injector = GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bind(LocalDataSegmentPuller.class);
              }
            }
        )
    );
    mapper = new DefaultObjectMapper();
    mapper.registerModule(new SimpleModule("loadSpecTest").registerSubtypes(LocalLoadSpec.class));
    mapper.setInjectableValues(new GuiceInjectableValues(injector));

    final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
    mapper.setAnnotationIntrospectors(
        new AnnotationIntrospectorPair(guiceIntrospector, mapper.getSerializationConfig().getAnnotationIntrospector()),
        new AnnotationIntrospectorPair(guiceIntrospector, mapper.getDeserializationConfig().getAnnotationIntrospector())
    );
  }

  @Test
  public void testStringResolve() throws IOException
  {
    LoadSpec loadSpec = mapper.readValue(value, LoadSpec.class);
    Assert.assertEquals(expectedId, loadSpec.getClass().getAnnotation(JsonTypeName.class).value());
  }
}
