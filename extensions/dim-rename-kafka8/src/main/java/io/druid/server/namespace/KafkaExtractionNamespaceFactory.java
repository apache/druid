/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.namespace;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.Runnables;
import com.google.inject.Inject;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.KafkaExtractionNamespace;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class KafkaExtractionNamespaceFactory implements ExtractionNamespaceFunctionFactory<KafkaExtractionNamespace>
{
  private final NamespaceExtractionCacheManager cacheManager;
  private final KafkaExtractionManager kafkaExtractionManager;
  @Inject
  public KafkaExtractionNamespaceFactory(
      final NamespaceExtractionCacheManager cacheManager,
      final KafkaExtractionManager kafkaExtractionManager
  ){
    this.cacheManager = cacheManager;
    this.kafkaExtractionManager = kafkaExtractionManager;
  }
  @Override
  public Function<String, String> build(KafkaExtractionNamespace extractionNamespace)
  {
    final ConcurrentMap<String, String> cache = cacheManager.getCacheMap(extractionNamespace.getNamespace());
    return new Function<String, String>()
    {
      @Nullable
      @Override
      public String apply(String input)
      {
        return cache.get(input);
      }
    };
  }

  @Override
  public Runnable getCachePopulator(final KafkaExtractionNamespace extractionNamespace)
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        kafkaExtractionManager.addListener(extractionNamespace);
      }
    };
  }
}
