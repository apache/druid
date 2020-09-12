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

package org.apache.druid.firehose.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.prefetch.PrefetchableTextFilesFirehoseFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.aliyun.OssStorageDruidModule;
import org.apache.druid.storage.aliyun.OssUtils;
import org.apache.druid.utils.CompressionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Builds firehoses that read from a predefined list of aliyun OSS objects and then dry up.
 */
public class StaticOssFirehoseFactory extends PrefetchableTextFilesFirehoseFactory<URI>
{
  private static final Logger log = new Logger(StaticOssFirehoseFactory.class);

  private final OSS client;
  private final List<URI> uris;
  private final List<URI> prefixes;

  @JsonCreator
  public StaticOssFirehoseFactory(
      @JacksonInject OSS client,
      @JsonProperty("uris") List<URI> uris,
      @JsonProperty("prefixes") List<URI> prefixes,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry
  )
  {
    super(maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    this.client = Preconditions.checkNotNull(client, "client");
    this.uris = uris == null ? new ArrayList<>() : uris;
    this.prefixes = prefixes == null ? new ArrayList<>() : prefixes;

    if (!this.uris.isEmpty() && !this.prefixes.isEmpty()) {
      throw new IAE("uris and prefixes cannot be used together");
    }

    if (this.uris.isEmpty() && this.prefixes.isEmpty()) {
      throw new IAE("uris or prefixes must be specified");
    }

    for (final URI inputURI : this.uris) {
      Preconditions.checkArgument(OssStorageDruidModule.SCHEME.equals(inputURI.getScheme()),
                                  "input uri scheme == %s (%s)",
                                  OssStorageDruidModule.SCHEME,
                                  inputURI);
    }

    for (final URI inputURI : this.prefixes) {
      Preconditions.checkArgument(OssStorageDruidModule.SCHEME.equals(inputURI.getScheme()),
                                  "input uri scheme == %s (%s)",
                                  OssStorageDruidModule.SCHEME,
                                  inputURI);
    }
  }

  @JsonProperty
  public List<URI> getUris()
  {
    return uris;
  }

  @JsonProperty("prefixes")
  public List<URI> getPrefixes()
  {
    return prefixes;
  }

  @Override
  protected Collection<URI> initObjects()
  {
    if (!uris.isEmpty()) {
      return uris;
    } else {
      final List<OSSObjectSummary> objects = new ArrayList<>();
      for (final URI prefix : prefixes) {
        final Iterator<OSSObjectSummary> objectSummaryIterator = OssUtils.objectSummaryIterator(
            client,
            Collections.singletonList(prefix),
            OssUtils.MAX_LISTING_LENGTH
        );

        objectSummaryIterator.forEachRemaining(objects::add);
      }
      return objects.stream().map(OssUtils::summaryToUri).collect(Collectors.toList());
    }
  }

  @Override
  protected InputStream openObjectStream(URI object) throws IOException
  {
    try {
      // Get data of the given object and open an input stream
      final String bucket = object.getAuthority();
      final String key = OssUtils.extractKey(object);

      final OSSObject ossObject = client.getObject(bucket, key);
      if (ossObject == null) {
        throw new ISE("Failed to get an Aliyun OSS object for bucket[%s] and key[%s]", bucket, key);
      }
      return ossObject.getObjectContent();
    }
    catch (OSSException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected InputStream openObjectStream(URI object, long start) throws IOException
  {
    final String bucket = object.getAuthority();
    final String key = OssUtils.extractKey(object);

    final GetObjectRequest request = new GetObjectRequest(bucket, key);
    try {
      final OSSObject ossObject = client.getObject(request);
      if (ossObject == null) {
        throw new ISE(
            "Failed to get an Aliyun OSS object for bucket[%s], key[%s], and start[%d]",
            bucket,
            key,
            start
        );
      }
      InputStream is = ossObject.getObjectContent();
      is.skip(start);
      return is;
    }
    catch (OSSException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected InputStream wrapObjectStream(URI object, InputStream stream) throws IOException
  {
    return CompressionUtils.decompress(stream, OssUtils.extractKey(object));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StaticOssFirehoseFactory that = (StaticOssFirehoseFactory) o;

    return Objects.equals(uris, that.uris) &&
           Objects.equals(prefixes, that.prefixes) &&
           getMaxCacheCapacityBytes() == that.getMaxCacheCapacityBytes() &&
           getMaxFetchCapacityBytes() == that.getMaxFetchCapacityBytes() &&
           getPrefetchTriggerBytes() == that.getPrefetchTriggerBytes() &&
           getFetchTimeout() == that.getFetchTimeout() &&
           getMaxFetchRetry() == that.getMaxFetchRetry();
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        uris,
        prefixes,
        getMaxCacheCapacityBytes(),
        getMaxFetchCapacityBytes(),
        getPrefetchTriggerBytes(),
        getFetchTimeout(),
        getMaxFetchRetry()
    );
  }

  @Override
  protected Predicate<Throwable> getRetryCondition()
  {
    return OssUtils.RETRYABLE;
  }

  @Override
  public FiniteFirehoseFactory<StringInputRowParser, URI> withSplit(InputSplit<URI> split)
  {
    return new StaticOssFirehoseFactory(
        client,
        Collections.singletonList(split.get()),
        null,
        getMaxCacheCapacityBytes(),
        getMaxFetchCapacityBytes(),
        getPrefetchTriggerBytes(),
        getFetchTimeout(),
        getMaxFetchRetry()
    );
  }
}
