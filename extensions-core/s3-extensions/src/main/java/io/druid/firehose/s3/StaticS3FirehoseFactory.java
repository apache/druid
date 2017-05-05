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
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds firehoses that read from a predefined list of S3 objects and then dry up.
 */
public class StaticS3FirehoseFactory extends PrefetchableTextFilesFirehoseFactory<S3Object>
{
  private static final Logger log = new Logger(StaticS3FirehoseFactory.class);

  private final RestS3Service s3Client;
  private final List<URI> uris;
  private final List<URI> directories;

  @JsonCreator
  public StaticS3FirehoseFactory(
      @JacksonInject("s3Client") RestS3Service s3Client,
      @JsonProperty("uris") List<URI> uris,
      @JsonProperty("directories") List<URI> directories,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry
  )
  {
    super(maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    this.s3Client = Preconditions.checkNotNull(s3Client, "null s3Client");
    this.uris = uris == null ? new ArrayList<>() : uris;
    this.directories = directories == null ? new ArrayList<>() : directories;

    if (!this.uris.isEmpty() && !this.directories.isEmpty()) {
      throw new IAE("uris and directories cannot be used together");
    }

    if (this.uris.isEmpty() && this.directories.isEmpty()) {
      throw new IAE("uris or directories must be specified");
    }

    for (final URI inputURI : this.uris) {
      Preconditions.checkArgument(inputURI.getScheme().equals("s3"), "input uri scheme == s3 (%s)", inputURI);
    }

    for (final URI inputURI : this.directories) {
      Preconditions.checkArgument(inputURI.getScheme().equals("s3"), "input uri scheme == s3 (%s)", inputURI);
    }
  }

  @JsonProperty
  public List<URI> getUris()
  {
    return uris;
  }

  @JsonProperty("directories")
  public List<URI> getDirectories()
  {
    return directories;
  }

  @Override
  protected Collection<S3Object> initObjects() throws IOException
  {
    // Here, the returned s3 objects contain minimal information without data.
    // Getting data is deferred until openObjectStream() is called for each object.
    if (!uris.isEmpty()) {
      return uris.stream()
          .map(
              uri -> {
                final String s3Bucket = uri.getAuthority();
                final S3Object s3Object = new S3Object(extractS3Key(uri));
                s3Object.setBucketName(s3Bucket);
                return s3Object;
              }
          )
          .collect(Collectors.toList());
    } else {
      final List<S3Object> objects = new ArrayList<>();
      for (URI uri : directories) {
        final String bucket = uri.getAuthority();
        final String prefix = extractS3Key(uri);
        try {
          final S3Object[] listed = s3Client.listObjects(bucket, prefix, null);
          objects.addAll(Arrays.asList(listed));
        }
        catch (S3ServiceException e) {
          throw new IOException(e);
        }
      }
      return objects;
    }
  }

  private static String extractS3Key(URI uri)
  {
    return uri.getPath().startsWith("/")
           ? uri.getPath().substring(1)
           : uri.getPath();
  }

  @Override
  protected InputStream openObjectStream(S3Object object) throws IOException
  {
    log.info("Reading from bucket[%s] object[%s] (%s)", object.getBucketName(), object.getKey(), object);

    try {
      // Get data of the given object and open an input stream
      return s3Client.getObject(object.getBucketName(), object.getKey()).getDataInputStream();
    }
    catch (ServiceException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected InputStream wrapObjectStream(S3Object object, InputStream stream) throws IOException
  {
    return object.getKey().endsWith(".gz") ? CompressionUtils.gzipInputStream(stream) : stream;
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
