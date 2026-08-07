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

package org.apache.druid.indexing.overlord.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.ProvisionException;
import com.google.inject.name.Names;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.IndexingServiceModuleHelper;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

public class ForkingTaskRunnerConfigTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final Injector INJECTOR = Initialization.makeInjectorWithModules(
      GuiceInjectors.makeStartupInjector(),
      ImmutableList.<Module>of(
          new Module()
          {
            @Override
            public void configure(Binder binder)
            {
              binder.bind(Key.get(String.class, Names.named("serviceName"))).toInstance("some service");
              binder.bind(Key.get(Integer.class, Names.named("servicePort"))).toInstance(0);
              binder.bind(Key.get(Integer.class, Names.named("tlsServicePort"))).toInstance(-1);
            }
          }
      )
  );
  private static final JsonConfigurator CONFIGURATOR = INJECTOR.getBinding(JsonConfigurator.class).getProvider().get();

  @Test
  public void testSimpleJavaOpts()
  {
    final ForkingTaskRunnerConfig forkingTaskRunnerConfig = CONFIGURATOR.configurate(
        new Properties(),
        "not found",
        ForkingTaskRunnerConfig.class
    );
    Assertions.assertEquals("", forkingTaskRunnerConfig.getJavaOpts());
    Assertions.assertEquals(ImmutableList.of(), forkingTaskRunnerConfig.getJavaOptsArray());
  }

  @Test
  public void testSimpleStringJavaOpts()
  {
    final String javaOpts = "some string";
    Assertions.assertEquals(
        javaOpts,
        buildFromProperties(ForkingTaskRunnerConfig.JAVA_OPTS_PROPERTY, javaOpts).getJavaOpts()
    );
  }

  @Test
  public void testCrazyQuotesStringJavaOpts()
  {
    final String javaOpts = "            \"test\",\n"
                            + "            \"-mmm\\\"some quote with\\\"suffix\",\n"
                            + "            \"test2\",\n"
                            + "            \"\\\"completely quoted\\\"\",\n"
                            + "            \"more\",\n"
                            + "            \"☃\",\n"
                            + "            \"-XX:SomeCoolOption=false\",\n"
                            + "            \"-XX:SomeOption=\\\"with spaces\\\"\",\n"
                            + "            \"someValues\",\n"
                            + "            \"some\\\"strange looking\\\"option\",\n"
                            + "            \"andOtherOptions\",\n"
                            + "            \"\\\"\\\"\",\n"
                            + "            \"AndMaybeEmptyQuotes\",\n"
                            + "            \"keep me around\"";
    Assertions.assertEquals(
        javaOpts,
        buildFromProperties(ForkingTaskRunnerConfig.JAVA_OPTS_PROPERTY, javaOpts).getJavaOpts()
    );
  }

  @Test
  public void testSimpleJavaOptArray() throws JsonProcessingException
  {
    final List<String> javaOpts = ImmutableList.of("option1", "option \"2\"");
    Assertions.assertEquals(
        javaOpts,
        buildFromProperties(
            ForkingTaskRunnerConfig.JAVA_OPTS_ARRAY_PROPERTY,
            MAPPER.writeValueAsString(javaOpts)
        ).getJavaOptsArray()
    );
  }

  @Test
  public void testCrazyJavaOptArray() throws JsonProcessingException
  {
    final List<String> javaOpts = ImmutableList.of(
        "test",
        "-mmm\"some quote with\"suffix",
        "test2",
        "\"completely quoted\"",
        "more",
        "☃",
        "-XX:SomeCoolOption=false",
        "-XX:SomeOption=\"with spaces\"",
        "someValues",
        "some\"strange looking\"option",
        "andOtherOptions",
        "\"\"",
        "AndMaybeEmptyQuotes",
        "keep me around"
    );
    Assertions.assertEquals(
        javaOpts,
        buildFromProperties(
            ForkingTaskRunnerConfig.JAVA_OPTS_ARRAY_PROPERTY,
            MAPPER.writeValueAsString(javaOpts)
        ).getJavaOptsArray()
    );
  }

  @Test
  public void testPorts() throws JsonProcessingException
  {
    final List<Integer> ports = ImmutableList.of(1024, 1025);
    Assertions.assertEquals(
        ports,
        buildFromProperties(
            IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX + ".ports",
            MAPPER.writeValueAsString(ports)
        ).getPorts()
    );
  }

  @Test
  public void testExceptionalPorts()
  {
    Assertions.assertThrows(
        ProvisionException.class,
        () -> buildFromProperties(IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX + ".ports", "not an Integer")
    );
  }

  @Test
  public void testExceptionalPorts2()
  {
    Assertions.assertThrows(
        ProvisionException.class,
        () -> buildFromProperties(IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX + ".ports", "1024")
    ); // not an array
  }

  @Test
  public void testExceptionalJavaOptArray()
  {
    Assertions.assertThrows(
        ProvisionException.class,
        () -> buildFromProperties(ForkingTaskRunnerConfig.JAVA_OPTS_ARRAY_PROPERTY, "not an array")
    );
  }

  @Test
  public void testExceptionalJavaOpt()
  {
    Assertions.assertThrows(
        ProvisionException.class,
        () -> buildFromProperties(ForkingTaskRunnerConfig.JAVA_OPTS_PROPERTY, "[\"not a string\"]")
    );
  }

  @Test
  public void testExceptionalJavaOpt2()
  {
    Assertions.assertThrows(
        ProvisionException.class,
        () -> buildFromProperties(ForkingTaskRunnerConfig.JAVA_OPTS_PROPERTY, "{\"not a string\":\"someVal\"}")
    );
  }

  private ForkingTaskRunnerConfig buildFromProperties(String key, String value)
  {
    final Properties properties = new Properties();
    properties.put(key, value);
    return CONFIGURATOR.configurate(
        properties,
        IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX,
        ForkingTaskRunnerConfig.class
    );
  }
}
