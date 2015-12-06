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

package io.druid.server.namespace;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.annotations.Json;
import io.druid.initialization.DruidModule;
import io.druid.query.extraction.namespace.KafkaExtractionNamespace;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class KafkaExtractionNamespaceModule implements DruidModule
{
  private static final String PROPERTIES_KEY = "druid.query.rename.kafka.properties";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.<Module>of(
        new SimpleModule("kafka-lookups"){
          @Override
          public void setupModule(SetupContext setupContext)
          {
            setupContext.registerSubtypes(KafkaExtractionNamespace.class);
            super.setupModule(setupContext);
          }
        }
    );
  }

  @Provides
  @Named("renameKafkaProperties")
  @LazySingleton
  public Properties getProperties(
      @Json ObjectMapper mapper,
      Properties systemProperties
  )
  {
    String val = systemProperties.getProperty(PROPERTIES_KEY);
    if (val == null) {
      return new Properties();
    }
    try {
      final Properties properties = new Properties();
      properties.putAll(
          mapper.<Map<String, String>>readValue(
              val, new TypeReference<Map<String, String>>()
              {
              }
          )
      );
      return properties;
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Provides
  @LazySingleton
  public KafkaExtractionNamespaceFactory factoryFactory(
      KafkaExtractionManager kafkaManager
  )
  {
    return new KafkaExtractionNamespaceFactory(kafkaManager);
  }

  @Override
  public void configure(Binder binder)
  {
    LifecycleModule.register(binder, KafkaExtractionManager.class);
    NamespacedExtractionModule
        .getNamespaceFactoryMapBinder(binder)
        .addBinding(KafkaExtractionNamespace.class)
        .to(KafkaExtractionNamespaceFactory.class)
        .in(LazySingleton.class);
  }
}
