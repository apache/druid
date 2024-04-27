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

package org.apache.druid.guice;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.junit.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DruidInjectorBuilderTest
{
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  public static class MockObject
  {
  }

  @JsonTypeName("extn")
  public static class MockObjectExtension extends MockObject
  {
  }

  public interface MockInterface
  {
  }

  public static class MockComponent implements MockInterface
  {
  }

  private static class MockGuiceModule implements com.google.inject.Module
  {
    @Inject
    public Properties properties;

    @Override
    public void configure(Binder binder)
    {
      binder.bind(MockInterface.class).to(MockComponent.class).in(LazySingleton.class);
    }
  }

  private static class MockDruidModule implements DruidModule
  {
    @Inject
    public Properties properties;

    @Override
    public void configure(Binder binder)
    {
    }

    @Override
    public List<? extends Module> getJacksonModules()
    {
      return ImmutableList.<Module>of(
          new SimpleModule("MockModule").registerSubtypes(MockObjectExtension.class)
      );
    }
  }

  @LoadScope(roles = NodeRole.BROKER_JSON_NAME)
  private static class MockRoleModule extends MockDruidModule
  {
  }

  @Test
  public void testEmpty()
  {
    Properties props = new Properties();
    props.put("foo", "bar");
    Injector injector = new CoreInjectorBuilder(
        new StartupInjectorBuilder()
          .forTests()
          .withProperties(props)
          .build()
    ).build();

    // Returns explicit properties
    Properties propsInstance = injector.getInstance(Properties.class);
    assertSame(props, propsInstance);
  }

  /**
   * Test the most generic form: addInput. Calls addModule() internally.
   */
  @Test
  public void testAddInputModules() throws IOException
  {
    Properties props = new Properties();
    props.put("foo", "bar");
    MockGuiceModule guiceModule = new MockGuiceModule();
    MockDruidModule druidModule = new MockDruidModule();
    Injector injector = new CoreInjectorBuilder(
        new StartupInjectorBuilder()
          .forTests()
          .withProperties(props)
          .build()
    )
        .addInput(guiceModule)
        .addInput(druidModule)
        .build();

    // Verify injection occurred
    assertSame(props, guiceModule.properties);
    assertSame(props, druidModule.properties);
    verifyInjector(injector);
  }

  private void verifyInjector(Injector injector) throws IOException
  {
    // Guice module did its thing
    assertTrue(injector.getInstance(MockInterface.class) instanceof MockComponent);

    // And that the Druid module set up Jackson.
    String json = "{\"type\": \"extn\"}";
    ObjectMapper om = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    MockObject obj = om.readValue(json, MockObject.class);
    assertTrue(obj instanceof MockObjectExtension);
  }

  /**
   * Test the ability to pass module classes rather than instances.
   */
  @Test
  public void testAddInputClasses() throws IOException
  {
    Properties props = new Properties();
    props.put("foo", "bar");
    Injector injector = new CoreInjectorBuilder(
        new StartupInjectorBuilder()
          .forTests()
          .withProperties(props)
          .build()
    )
        .addInput(MockGuiceModule.class)
        .addInput(MockDruidModule.class)
        .build();

    // Can't verify injection here, sadly

    verifyInjector(injector);
  }

  @Test
  public void testBadModule()
  {
    DruidInjectorBuilder builder = new CoreInjectorBuilder(
        new StartupInjectorBuilder()
          .forTests()
          .withEmptyProperties()
          .build()
    );
    assertThrows(ISE.class, () -> builder.addInput("I'm not a module"));
  }

  @Test
  public void testBadModuleClass()
  {
    DruidInjectorBuilder builder = new CoreInjectorBuilder(
        new StartupInjectorBuilder()
          .forTests()
          .withEmptyProperties()
          .build()
    );
    assertThrows(ISE.class, () -> builder.addInput(Object.class));
  }

  @Test
  public void testAddModules() throws IOException
  {
    Injector injector = new CoreInjectorBuilder(
        new StartupInjectorBuilder()
          .forTests()
          .withEmptyProperties()
          .build()
    )
        .addModules(new MockGuiceModule(), new MockDruidModule())
        .build();

    verifyInjector(injector);
  }

  @Test
  public void testAddAll() throws IOException
  {
    Injector injector = new CoreInjectorBuilder(
        new StartupInjectorBuilder()
          .forTests()
          .withEmptyProperties()
          .build()
    )
        .addAll(Arrays.asList(new MockGuiceModule(), new MockDruidModule()))
        .build();

    verifyInjector(injector);
  }

  /**
   * Enable extensions. Then, exclude our JSON test module. As a result, the
   * JSON object will fail to deserialize.
   */
  @Test
  public void testExclude()
  {
    Properties props = new Properties();
    props.put(ModulesConfig.PROPERTY_BASE + ".excludeList", "[\"" + MockDruidModule.class.getName() + "\"]");
    Injector injector = new CoreInjectorBuilder(
        new StartupInjectorBuilder()
          .withExtensions()
          .withProperties(props)
          .build()
    )
        .addInput(MockGuiceModule.class)
        .addInput(MockDruidModule.class)
        .build();

    assertThrows(IOException.class, () -> verifyInjector(injector));
  }

  @Test
  public void testMatchingNodeRole() throws IOException
  {
    Injector injector = new CoreInjectorBuilder(
        new StartupInjectorBuilder()
          .forTests()
          .withEmptyProperties()
          .build(),
        ImmutableSet.of(NodeRole.BROKER)
    )
        .addModules(new MockGuiceModule(), new MockRoleModule())
        .build();

    verifyInjector(injector);
  }

  @Test
  public void testNotMatchingNodeRole()
  {
    Injector injector = new CoreInjectorBuilder(
        new StartupInjectorBuilder()
          .forTests()
          .withEmptyProperties()
          .build(),
        ImmutableSet.of(NodeRole.COORDINATOR)
    )
        .addModules(new MockGuiceModule(), new MockRoleModule())
        .build();

    assertThrows(IOException.class, () -> verifyInjector(injector));
  }

  @Test
  public void testIgnoreNodeRole() throws IOException
  {
    Injector injector = new CoreInjectorBuilder(
        new StartupInjectorBuilder()
          .forTests()
          .withEmptyProperties()
          .build(),
        ImmutableSet.of(NodeRole.COORDINATOR)
    )
        .ignoreLoadScopes()
        .addModules(new MockGuiceModule(), new MockRoleModule())
        .build();

    verifyInjector(injector);
  }
}
