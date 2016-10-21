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

package io.druid.server.lookup.namespace;

import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;

import io.druid.common.utils.JodaUtils;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.data.input.MapPopulator;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.query.lookup.namespace.URIExtractionNamespace;
import io.druid.segment.loading.URIDataPuller;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 *
 */
public class URIExtractionNamespaceCacheFactory implements ExtractionNamespaceCacheFactory<URIExtractionNamespace>
{
  private static final int DEFAULT_NUM_RETRIES = 3;
  private static final Logger log = new Logger(URIExtractionNamespaceCacheFactory.class);
  private final Map<String, SearchableVersionedDataFinder> pullers;

  @Inject
  public URIExtractionNamespaceCacheFactory(
      Map<String, SearchableVersionedDataFinder> pullers
  )
  {
    this.pullers = pullers;
  }

  @Override
  public Callable<String> getCachePopulator(
      final String id,
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
        final boolean doSearch = extractionNamespace.getUriPrefix() != null;
        final URI originalUri = doSearch ? extractionNamespace.getUriPrefix() : extractionNamespace.getUri();
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
              originalUri,
              originalUri.getScheme()
          );
        }
        final URIDataPuller puller = (URIDataPuller) pullerRaw;
        final URI uri;
        if (doSearch) {
          final Pattern versionRegex;

          if (extractionNamespace.getFileRegex() != null) {
            versionRegex = Pattern.compile(extractionNamespace.getFileRegex());
          } else {
            versionRegex = null;
          }
          uri = pullerRaw.getLatestVersion(
              extractionNamespace.getUriPrefix(),
              versionRegex
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
        } else {
          uri = extractionNamespace.getUri();
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
                          id,
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
                      id
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
