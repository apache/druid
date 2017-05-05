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

package io.druid.firehose.s3;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.data.input.impl.PrefetchableTextFilesFirehoseFactory;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.logger.Logger;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;

/**
 * Builds firehoses that read from a predefined list of S3 objects and then dry up.
 */
public class StaticS3FirehoseFactory extends PrefetchableTextFilesFirehoseFactory<URI>
{
  private static final Logger log = new Logger(StaticS3FirehoseFactory.class);

  private final RestS3Service s3Client;
  private final List<URI> uris;

  @JsonCreator
  public StaticS3FirehoseFactory(
      @JacksonInject("s3Client") RestS3Service s3Client,
      @JsonProperty("uris") List<URI> uris,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry
  )
  {
    super(maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    this.s3Client = Preconditions.checkNotNull(s3Client, "null s3Client");
    this.uris = uris;

    for (final URI inputURI : uris) {
      Preconditions.checkArgument(inputURI.getScheme().equals("s3"), "input uri scheme == s3 (%s)", inputURI);
    }
  }

  @JsonProperty
  public List<URI> getUris()
  {
    return uris;
  }

  @Override
  protected Collection<URI> initObjects()
  {
    return uris;
  }

  @Override
  protected InputStream openObjectStream(URI object) throws IOException
  {
    final String s3Bucket = object.getAuthority();
    final S3Object s3Object = new S3Object(
        object.getPath().startsWith("/")
        ? object.getPath().substring(1)
        : object.getPath()
    );

    log.info("Reading from bucket[%s] object[%s] (%s)", s3Bucket, s3Object.getKey(), object);

    try {
      return s3Client.getObject(new S3Bucket(s3Bucket), s3Object.getKey()).getDataInputStream();
    }
    catch (ServiceException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected InputStream wrapObjectStream(URI object, InputStream stream) throws IOException
  {
    return object.getPath().endsWith(".gz") ? CompressionUtils.gzipInputStream(stream) : stream;
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

    StaticS3FirehoseFactory factory = (StaticS3FirehoseFactory) o;

    return getUris().equals(factory.getUris());

  }

  @Override
  public int hashCode()
  {
    return getUris().hashCode();
  }
}
