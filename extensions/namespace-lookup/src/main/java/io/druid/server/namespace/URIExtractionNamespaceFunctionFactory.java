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
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import com.metamx.common.CompressionUtils;
import com.metamx.common.IAE;
import com.metamx.common.RetryUtils;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.data.input.MapPopulator;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.segment.loading.URIDataPuller;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 *
 */
public class URIExtractionNamespaceFunctionFactory implements ExtractionNamespaceFunctionFactory<URIExtractionNamespace>
{
  private static final int DEFAULT_NUM_RETRIES = 3;
  private static final Logger log = new Logger(URIExtractionNamespaceFunctionFactory.class);
  private final Map<String, SearchableVersionedDataFinder> pullers;

  @Inject
  public URIExtractionNamespaceFunctionFactory(
      Map<String, SearchableVersionedDataFinder> pullers
  )
  {
    this.pullers = pullers;
  }

  @Override
  public Function<String, String> buildFn(URIExtractionNamespace extractionNamespace, final Map<String, String> cache)
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
      URIExtractionNamespace extractionNamespace, final Map<String, String> cache
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
          @Override
          public boolean apply(@Nullable String key)
          {
            return cache.get(key).equals(Strings.nullToEmpty(value));
          }
        }).keySet());
      }
    };
  }

  @Override
  public Callable<String> getCachePopulator(
      final URIExtractionNamespace extractionNamespace,
      final String lastVersion,
      final Map<String, String> cache
  )
  {
    final long lastCached = lastVersion == null ? JodaUtils.MIN_INSTANT : Long.parseLong(lastVersion);
    return new Callable<String>()
    {
      @Override
      public String call()
      {
        final URI originalUri = extractionNamespace.getUri();
        final SearchableVersionedDataFinder<URI> pullerRaw = pullers.get(originalUri.getScheme());
        if (pullerRaw == null) {
          throw new IAE(
              "Unknown loader type[%s].  Known types are %s",
              originalUri.getScheme(),
              pullers.keySet()
          );
        }
        if (!(pullerRaw instanceof URIDataPuller)) {
          throw new IAE(
              "Cannot load data from location [%s]. Data pulling from [%s] not supported",
              originalUri.toString(),
              originalUri.getScheme()
          );
        }
        final URIDataPuller puller = (URIDataPuller) pullerRaw;
        final String versionRegex = extractionNamespace.getVersionRegex();
        final URI uri = pullerRaw.getLatestVersion(
            originalUri,
            versionRegex == null ? null : Pattern.compile(versionRegex)
        );
        if (uri == null) {
          throw new RuntimeException(
              new FileNotFoundException(
                  String.format(
                      "Could not find match for pattern `%s` in [%s] for %s",
                      versionRegex,
                      originalUri,
                      extractionNamespace
                  )
              )
          );
        }
        final String uriPath = uri.getPath();

        try {
          return RetryUtils.retry(
              new Callable<String>()
              {
                @Override
                public String call() throws Exception
                {
                  final String version = puller.getVersion(uri);
                  try {
                    long lastModified = Long.parseLong(version);
                    if (lastModified <= lastCached) {
                      final DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
                      log.debug(
                          "URI [%s] for namespace [%s] was las modified [%s] but was last cached [%s]. Skipping ",
                          uri.toString(),
                          extractionNamespace.getNamespace(),
                          fmt.print(lastModified),
                          fmt.print(lastCached)
                      );
                      return version;
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
                  final long lineCount = new MapPopulator<>(
                      extractionNamespace.getNamespaceParseSpec()
                                         .getParser()
                  ).populate(source, cache);
                  log.info(
                      "Finished loading %d lines for namespace [%s]",
                      lineCount,
                      extractionNamespace.getNamespace()
                  );
                  return version;
                }
              },
              puller.shouldRetryPredicate(),
              DEFAULT_NUM_RETRIES
          );
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}
