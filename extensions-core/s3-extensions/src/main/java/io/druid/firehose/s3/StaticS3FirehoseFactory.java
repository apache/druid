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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import io.druid.data.input.impl.prefetch.PrefetchableTextFilesFirehoseFactory;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.storage.s3.S3Utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Builds firehoses that read from a predefined list of S3 objects and then dry up.
 */
public class StaticS3FirehoseFactory extends PrefetchableTextFilesFirehoseFactory<S3ObjectSummary>
{
  private static final Logger log = new Logger(StaticS3FirehoseFactory.class);
  private static final int MAX_LISTING_LENGTH = 1024;

  private final AmazonS3 s3Client;
  private final List<URI> uris;
  private final List<URI> prefixes;

  @JsonCreator
  public StaticS3FirehoseFactory(
      @JacksonInject("s3Client") AmazonS3 s3Client,
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
    this.s3Client = Preconditions.checkNotNull(s3Client, "s3Client");
    this.uris = uris == null ? new ArrayList<>() : uris;
    this.prefixes = prefixes == null ? new ArrayList<>() : prefixes;

    if (!this.uris.isEmpty() && !this.prefixes.isEmpty()) {
      throw new IAE("uris and prefixes cannot be used together");
    }

    if (this.uris.isEmpty() && this.prefixes.isEmpty()) {
      throw new IAE("uris or prefixes must be specified");
    }

    for (final URI inputURI : this.uris) {
      Preconditions.checkArgument(inputURI.getScheme().equals("s3"), "input uri scheme == s3 (%s)", inputURI);
    }

    for (final URI inputURI : this.prefixes) {
      Preconditions.checkArgument(inputURI.getScheme().equals("s3"), "input uri scheme == s3 (%s)", inputURI);
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
  protected Collection<S3ObjectSummary> initObjects() throws IOException
  {
    // Here, the returned s3 objects contain minimal information without data.
    // Getting data is deferred until openObjectStream() is called for each object.
    if (!uris.isEmpty()) {
      return uris.stream()
                 .map(
                     uri -> {
                       final String s3Bucket = uri.getAuthority();
                       final String key = S3Utils.extractS3Key(uri);
                       return S3Utils.getSingleObjectSummary(s3Client, s3Bucket, key);
                     }
                 )
                 .collect(Collectors.toList());
    } else {
      final List<S3ObjectSummary> objects = new ArrayList<>();
      for (URI uri : prefixes) {
        final String bucket = uri.getAuthority();
        final String prefix = S3Utils.extractS3Key(uri);

        try {
          final Iterator<S3ObjectSummary> objectSummaryIterator = S3Utils.objectSummaryIterator(
              s3Client,
              bucket,
              prefix,
              MAX_LISTING_LENGTH
          );
          objects.addAll(Lists.newArrayList(objectSummaryIterator));
        }
        catch (AmazonS3Exception outerException) {
          log.error(outerException, "Exception while listing on %s", uri);

          if (outerException.getStatusCode() == 403) {
            // The "Access Denied" means users might not have a proper permission for listing on the given uri.
            // Usually this is not a problem, but the uris might be the full paths to input objects instead of prefixes.
            // In this case, users should be able to get objects if they have a proper permission for GetObject.

            log.warn("Access denied for %s. Try to get the object from the uri without listing", uri);
            try {
              final ObjectMetadata objectMetadata = s3Client.getObjectMetadata(bucket, prefix);

              if (!S3Utils.isDirectoryPlaceholder(prefix, objectMetadata)) {
                objects.add(S3Utils.getSingleObjectSummary(s3Client, bucket, prefix));
              } else {
                throw new IOE(
                    "[%s] is a directory placeholder, "
                    + "but failed to get the object list under the directory due to permission",
                    uri
                );
              }
            }
            catch (AmazonS3Exception innerException) {
              throw new IOException(innerException);
            }
          } else {
            throw new IOException(outerException);
          }
        }
      }
      return objects;
    }
  }

  @Override
  protected InputStream openObjectStream(S3ObjectSummary object) throws IOException
  {
    try {
      // Get data of the given object and open an input stream
      final S3Object s3Object = s3Client.getObject(object.getBucketName(), object.getKey());
      if (s3Object == null) {
        throw new ISE("Failed to get an s3 object for bucket[%s] and key[%s]", object.getBucketName(), object.getKey());
      }
      return s3Object.getObjectContent();
    }
    catch (AmazonS3Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected InputStream openObjectStream(S3ObjectSummary object, long start) throws IOException
  {
    final GetObjectRequest request = new GetObjectRequest(object.getBucketName(), object.getKey());
    request.setRange(start);
    try {
      final S3Object s3Object = s3Client.getObject(request);
      if (s3Object == null) {
        throw new ISE(
            "Failed to get an s3 object for bucket[%s], key[%s], and start[%d]",
            object.getBucketName(),
            object.getKey(),
            start
        );
      }
      return s3Object.getObjectContent();
    }
    catch (AmazonS3Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected InputStream wrapObjectStream(S3ObjectSummary object, InputStream stream) throws IOException
  {
    return CompressionUtils.decompress(stream, object.getKey());
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

    StaticS3FirehoseFactory that = (StaticS3FirehoseFactory) o;

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
    return S3Utils.S3RETRY;
  }
}
