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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.common.io.ByteSource;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import com.metamx.common.CompressionUtils;
import com.metamx.common.IAE;
import com.metamx.common.RetryUtils;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.MapPopulator;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.URIDataPuller;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 *
 */
public class URIExtractionNamespaceFunctionFactory implements ExtractionNamespaceFunctionFactory<URIExtractionNamespace>
{
  private static final Logger log = new Logger(URIExtractionNamespaceFunctionFactory.class);
  private final NamespaceExtractionCacheManager extractionCacheManager;
  private final ObjectMapper smileMapper;
  private final ObjectMapper jsomMapper;
  private final Map<String, DataSegmentPuller> pullers;

  @Inject
  public URIExtractionNamespaceFunctionFactory(
      NamespaceExtractionCacheManager extractionCacheManager,
      @Smile ObjectMapper smileMapper,
      @Json ObjectMapper jsonMapper,
      Map<String, DataSegmentPuller> pullers
  )
  {
    this.extractionCacheManager = extractionCacheManager;
    this.smileMapper = smileMapper;
    this.jsomMapper = jsonMapper;
    this.pullers = pullers;
  }

  @Override
  public Function<String, String> build(final URIExtractionNamespace extractionNamespace)
  {
    final ConcurrentMap<String, String> cache = extractionCacheManager.getCacheMap(extractionNamespace.getNamespace());
    return new Function<String, String>()
    {
      @Nullable
      @Override
      public String apply(String input)
      {
        if (input == null) {
          return null;
        }
        return cache.get(input);
      }
    };
  }

  @Override
  public Runnable getCachePopulator(final URIExtractionNamespace extractionNamespace)
  {
    return new Runnable()
    {
      private volatile long lastCached = JodaUtils.MIN_INSTANT;
      @Override
      public void run()
      {
        final URI uri = extractionNamespace.getUri();
        final DataSegmentPuller pullerRaw = pullers.get(uri.getScheme());
        if (pullerRaw == null) {
          throw new IAE(
              "Unknown loader type[%s].  Known types are %s",
              uri.getScheme(),
              pullers.keySet()
          );
        }
        if(!(pullerRaw instanceof URIDataPuller)){
          throw new IAE("Cannot load data from location [%s]. Data pulling from [%s] not supported", uri.toString(), uri.getScheme());
        }
        final URIDataPuller puller = (URIDataPuller)pullerRaw;
        final String uriPath = uri.getPath();

        // Inspired by OmniSegmentLoader
        try {
          RetryUtils.retry(
              new Callable<Void>()
              {
                @Override
                public Void call() throws Exception
                {
                  final String version = puller.getVersion(uri);
                  Long lastModified = null;
                  try {
                    lastModified = Long.parseLong(version);
                    if (lastModified <= lastCached) {
                      final DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
                      log.info(
                          "URI [%s] for namespace [%s] was las modified [%s] but was last cached [%s]. Skipping ",
                          uri.toString(),
                          extractionNamespace.getNamespace(),
                          fmt.print(lastModified),
                          fmt.print(lastCached)
                      );
                      return null;
                    }
                  }
                  catch (NumberFormatException ex) {
                    log.debug(ex, "Failed to get last modified timestamp. Assuming no timestamp");
                  }
                  final ByteSource source;
                  if (CompressionUtils.isGz(uriPath)) {
                    // Simple gzip stream
                    log.debug("Loading gz");
                    source = new ByteSource()
                    {
                      @Override
                      public InputStream openStream() throws IOException
                      {
                        return CompressionUtils.gzipInputStream(puller.getInputStream(uri));
                      }
                    };
                  } else {
                    source = new ByteSource()
                    {
                      @Override
                      public InputStream openStream() throws IOException
                      {
                        return puller.getInputStream(uri);
                      }
                    };
                  }
                  final MapPopulator<String, String> populator = new MapPopulator<>(extractionNamespace.getParseSpec().getParser());
                  populator.populate(source, extractionCacheManager.getCacheMap(extractionNamespace.getNamespace()));
                  log.info("Finished loading namespace [%s]", extractionNamespace.getNamespace());
                  if (lastModified != null) {
                    lastCached = lastModified;
                  }
                  return null;
                }
              },
              puller.shouldRetryPredicate(),
              10
          );
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}
