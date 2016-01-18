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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.KafkaExtractionNamespace;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 *
 */
public class KafkaExtractionNamespaceFactory implements ExtractionNamespaceFunctionFactory<KafkaExtractionNamespace>
{
  private final KafkaExtractionManager kafkaExtractionManager;
  private static final String KAFKA_VERSION = "kafka versions are updated every time a new event comes in";

  @Inject
  public KafkaExtractionNamespaceFactory(
      final KafkaExtractionManager kafkaExtractionManager
  )
  {
    this.kafkaExtractionManager = kafkaExtractionManager;
  }


  @Override
  public Function<String, String> buildFn(KafkaExtractionNamespace extractionNamespace, final Map<String, String> cache)
  {
    return new Function<String, String>()
    {
      @Nullable
      @Override
      public String apply(String input)
      {
        if (Strings.isNullOrEmpty(input)) {
          return null;
        }
        return Strings.emptyToNull(cache.get(input));
      }
    };
  }

  @Override
  public Function<String, List<String>> buildReverseFn(
      KafkaExtractionNamespace extractionNamespace, final Map<String, String> cache
  )
  {
    return new Function<String, List<String>>()
    {
      @Nullable
      @Override
      public List<String> apply(@Nullable final String value)
      {
        return Lists.newArrayList(Maps.filterKeys(cache, new Predicate<String>()
        {
          @Override public boolean apply(@Nullable String key)
          {
            return cache.get(key).equals(Strings.nullToEmpty(value));
          }
        }).keySet());
      }
    };
  }

  // This only fires ONCE when the namespace is first added. The version is updated externally as events come in
  @Override
  public Callable<String> getCachePopulator(
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
        kafkaExtractionManager.addListener(extractionNamespace, cache);
        return KAFKA_VERSION;
      }
    };
  }
}
