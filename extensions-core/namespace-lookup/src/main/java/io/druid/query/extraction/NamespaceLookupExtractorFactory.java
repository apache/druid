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
package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.inject.name.Named;
import com.metamx.common.logger.Logger;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.lookup.LookupExtractor;
import io.druid.query.lookup.LookupExtractorFactory;
import io.druid.server.namespace.NamespacedExtractionModule;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@JsonTypeName("namespace")
public class NamespaceLookupExtractorFactory implements LookupExtractorFactory
{
  private static final Logger log = new Logger(NamespaceLookupExtractorFactory.class);

  private static AtomicInteger numExtractor = new AtomicInteger(0);
  private static long SCHEDULE_TIMEOUT = 60_000;

  final ExtractionNamespace extractionNamespace;
  final NamespaceExtractionCacheManager manager;
  final Function<String, Function<String, String>> fnMaker;
  final Function<String, Function<String, List<String>>> reverseFnMaker;
  LookupExtractor lookupExtractor;

  private final String extractorID;

  @JsonCreator
  public NamespaceLookupExtractorFactory(
      @JsonProperty("extractionNamespace")ExtractionNamespace extractionNamespace,
      @JacksonInject @Named(NamespacedExtractionModule.EXTRACTION_CACHE_MANAGER)
      NamespaceExtractionCacheManager manager,
      @JacksonInject @Named(NamespacedExtractionModule.DIM_EXTRACTION_NAMESPACE)
      Function<String, Function<String, String>> fnMaker,
      @JacksonInject @Named(NamespacedExtractionModule.DIM_REVERSE_EXTRACTION_NAMESPACE)
      Function<String, Function<String, List<String>>> reverseFnMaker
      )
  {
    this.extractionNamespace = Preconditions.checkNotNull(extractionNamespace,
        "extractionNamespace should be specified");
    this.manager = manager;
    this.fnMaker = fnMaker;
    this.reverseFnMaker = reverseFnMaker;
    this.extractorID = getID();
  }

  @Override
  public boolean start()
  {
    if (!manager.scheduleAndWait(extractorID, extractionNamespace, SCHEDULE_TIMEOUT))
    {
      return false;
    }

    log.debug("NamespaceLookupExtractorFactory[%s] started", extractorID);
    this.lookupExtractor = new NamespacedExtractor(fnMaker, reverseFnMaker, extractorID);

    return true;
  }

  @Override
  public boolean close()
  {
    manager.checkedDelete(extractorID);
    return true;
  }

  @Override
  public boolean replaces(@Nullable LookupExtractorFactory other)
  {
    if (other != null && other instanceof NamespaceLookupExtractorFactory) {
      NamespaceLookupExtractorFactory that = (NamespaceLookupExtractorFactory) other;
      return !extractionNamespace.equals(that.extractionNamespace);
    }
    return true;
  }

  private String getID()
  {
    return String.format("%d - %s", numExtractor.getAndIncrement(), extractionNamespace);
  }

  @Override
  public LookupExtractor get()
  {
    return lookupExtractor;
  }
}
