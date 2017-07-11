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

package io.druid.indexing.overlord.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.ProvisionException;
import com.google.inject.name.Names;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.IndexingServiceModuleHelper;
import io.druid.guice.JsonConfigurator;
import io.druid.initialization.Initialization;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

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
    Assert.assertEquals("", forkingTaskRunnerConfig.getJavaOpts());
    Assert.assertEquals(ImmutableList.of(), forkingTaskRunnerConfig.getJavaOptsArray());
  }

  @Test
  public void testSimpleStringJavaOpts()
  {
    final String javaOpts = "some string";
    Assert.assertEquals(
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
    Assert.assertEquals(
        javaOpts,
        buildFromProperties(ForkingTaskRunnerConfig.JAVA_OPTS_PROPERTY, javaOpts).getJavaOpts()
    );
  }

  @Test
  public void testSimpleJavaOptArray() throws JsonProcessingException
  {
    final List<String> javaOpts = ImmutableList.of("option1", "option \"2\"");
    Assert.assertEquals(
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
    Assert.assertEquals(
        javaOpts,
        buildFromProperties(
            ForkingTaskRunnerConfig.JAVA_OPTS_ARRAY_PROPERTY,
            MAPPER.writeValueAsString(javaOpts)
        ).getJavaOptsArray()
    );
  }

  @Test(expected = ProvisionException.class)
  public void testExceptionalJavaOptArray() throws JsonProcessingException
  {
    buildFromProperties(ForkingTaskRunnerConfig.JAVA_OPTS_ARRAY_PROPERTY, "not an array");
  }

  @Test(expected = ProvisionException.class)
  public void testExceptionalJavaOpt() throws JsonProcessingException
  {
    buildFromProperties(ForkingTaskRunnerConfig.JAVA_OPTS_PROPERTY, "[\"not a string\"]");
  }

  @Test(expected = ProvisionException.class)
  public void testExceptionalJavaOpt2() throws JsonProcessingException
  {
    buildFromProperties(ForkingTaskRunnerConfig.JAVA_OPTS_PROPERTY, "{\"not a string\":\"someVal\"}");
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
