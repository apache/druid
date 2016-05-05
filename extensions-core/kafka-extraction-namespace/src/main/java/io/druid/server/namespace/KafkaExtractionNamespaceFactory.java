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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.druid.query.extraction.namespace.ExtractionNamespaceCacheFactory;
import io.druid.query.extraction.namespace.KafkaExtractionNamespace;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 *
 */
public class KafkaExtractionNamespaceFactory implements ExtractionNamespaceCacheFactory<KafkaExtractionNamespace>
{
  private final List<KafkaExtractionManager> kafkaExtractionManagers;
  private static final String KAFKA_VERSION = "kafka versions are updated every time a new event comes in";

  @Inject
  public KafkaExtractionNamespaceFactory(
      @JacksonInject @Named("kafkaManagers") final List<KafkaExtractionManager> kafkaExtractionManagers
  )
  {
    this.kafkaExtractionManagers = kafkaExtractionManagers;
  }

  // This only fires ONCE when the namespace is first added. The version is updated externally as events come in
  @Override
  public Callable<String> getCachePopulator(
      final String id,
      final KafkaExtractionNamespace extractionNamespace,
      final String unused,
      final Map<String, String> cache
  )
  {
    return new Callable<String>()
    {
      @Override
      public String call()
      {
        KafkaExtractionManager manager;
        synchronized (kafkaExtractionManagers) {
          manager = findAppropriate(extractionNamespace.getKafkaProperties());
          if (manager == null) {
            manager = new KafkaExtractionManager(extractionNamespace.getKafkaProperties());
            kafkaExtractionManagers.add(manager);
          }
        }
        manager.addListener(id, extractionNamespace, cache);
        return KAFKA_VERSION;
      }
    };
  }

  private KafkaExtractionManager findAppropriate(Properties kafkaProperties)
  {
    for (KafkaExtractionManager manager : kafkaExtractionManagers) {
      if (manager.supports(kafkaProperties)) {
        return manager;
      }
    }
    return null;
  }
}
