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

package org.apache.druid.data.input.s3;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class S3InputSource extends AbstractInputSource implements SplittableInputSource<URI>
{
  private static final Logger log = new Logger(S3InputSource.class);
  private static final int MAX_LISTING_LENGTH = 1024;

  private final ServerSideEncryptingAmazonS3 s3Client;
  private final List<URI> uris;
  private final List<URI> prefixes;

  @JsonCreator
  public S3InputSource(
      @JacksonInject ServerSideEncryptingAmazonS3 s3Client,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes
  )
  {
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
      Preconditions.checkArgument("s3".equals(inputURI.getScheme()), "input uri scheme == s3 (%s)", inputURI);
    }

    for (final URI inputURI : this.prefixes) {
      Preconditions.checkArgument("s3".equals(inputURI.getScheme()), "input uri scheme == s3 (%s)", inputURI);
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
  public Stream<InputSplit<URI>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    if (!uris.isEmpty()) {
      return uris.stream().map(InputSplit::new);
    }

    return StreamSupport.stream(getIterableObjectsFromPrefixes().spliterator(), false);
  }

  @Override
  public int getNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    if (!uris.isEmpty()) {
      return uris.size();
    }

    return (int) StreamSupport.stream(getIterableObjectsFromPrefixes().spliterator(), false).count();
  }

  @Override
  public SplittableInputSource<URI> withSplit(InputSplit<URI> split)
  {
    return new S3InputSource(s3Client, ImmutableList.of(split.get()), ImmutableList.of());
  }

  @Override
  public boolean needsFormat()
  {
    return true;
  }

  @Override
  protected InputSourceReader formattableReader(
      InputRowSchema inputRowSchema,
      InputFormat inputFormat,
      @Nullable File temporaryDirectory
  )
  {
    return new InputEntityIteratingReader(
        inputRowSchema,
        inputFormat,
        // formattableReader() is supposed to be called in each task that actually creates segments.
        // The task should already have only one split in parallel indexing,
        // while there's no need to make splits using splitHintSpec in sequential indexing.
        createSplits(inputFormat, null).map(split -> new S3Entity(s3Client, split.get())),
        temporaryDirectory
    );
  }


  /**
   * Create an {@link URI} from the given {@link S3ObjectSummary}. The result URI is composed as below.
   *
   * <pre>
   * {@code s3://{BUCKET_NAME}/{OBJECT_KEY}}
   * </pre>
   */
  public static URI toUri(S3ObjectSummary object)
  {
    final String originalAuthority = object.getBucketName();
    final String originalPath = object.getKey();
    final String authority = originalAuthority.endsWith("/") ?
                             originalAuthority.substring(0, originalAuthority.length() - 1) :
                             originalAuthority;
    final String path = originalPath.startsWith("/") ? originalPath.substring(1) : originalPath;

    return URI.create(StringUtils.format("s3://%s/%s", authority, path));
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
    S3InputSource that = (S3InputSource) o;
    return Objects.equals(uris, that.uris) &&
           Objects.equals(prefixes, that.prefixes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(uris, prefixes);
  }

  @Override
  public String toString()
  {
    return "S3InputSource{" +
           "uris=" + uris +
           ", prefixes=" + prefixes +
           '}';
  }

  private Iterable<InputSplit<URI>> getIterableObjectsFromPrefixes()
  {
    return () -> objectFetchingIterator(s3Client, prefixes.iterator());
  }

  private static Iterator<InputSplit<URI>> objectFetchingIterator(
      final ServerSideEncryptingAmazonS3 s3Client,
      final Iterator<URI> prefixes
  )
  {
    return new Iterator<InputSplit<URI>>()
    {
      private ListObjectsV2Request request;
      private ListObjectsV2Result result;
      private URI currentUri;
      private String currentBucket;
      private String currentPrefix;
      private Iterator<S3ObjectSummary> objectSummaryIterator;

      {
        prepareNextRequest();
        fetchNextBatch();
      }

      private void prepareNextRequest()
      {
        currentUri = prefixes.next();
        currentBucket = currentUri.getAuthority();
        currentPrefix = S3Utils.extractS3Key(currentUri);

        request = new ListObjectsV2Request()
            .withBucketName(currentBucket)
            .withPrefix(currentPrefix)
            .withMaxKeys(S3InputSource.MAX_LISTING_LENGTH);
      }

      private void fetchNextBatch()
      {
        try {
          result = S3Utils.retryS3Operation(() -> s3Client.listObjectsV2(request));
          objectSummaryIterator = result.getObjectSummaries().iterator();
          request.setContinuationToken(result.getContinuationToken());
        }
        catch (AmazonS3Exception outerException) {
          log.error(outerException, "Exception while listing on %s", currentUri);

          if (outerException.getStatusCode() == 403) {
            // The "Access Denied" means users might not have a proper permission for listing on the given uri.
            // Usually this is not a problem, but the uris might be the full paths to input objects instead of prefixes.
            // In this case, users should be able to get objects if they have a proper permission for GetObject.

            log.warn("Access denied for %s. Try to get the object from the uri without listing", currentUri);
            try {
              final ObjectMetadata objectMetadata =
                  S3Utils.retryS3Operation(() -> s3Client.getObjectMetadata(currentBucket, currentPrefix));

              if (!S3Utils.isDirectoryPlaceholder(currentPrefix, objectMetadata)) {

                objectSummaryIterator = Iterators.singletonIterator(
                    S3Utils.getSingleObjectSummary(s3Client, currentBucket, currentPrefix)
                );
              } else {
                throw new RE(
                    "[%s] is a directory placeholder, "
                    + "but failed to get the object list under the directory due to permission",
                    currentUri
                );
              }
            }
            catch (Exception innerException) {
              throw new RuntimeException(innerException);
            }
          } else {
            throw new RuntimeException(outerException);
          }
        }
        catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }

      @Override
      public boolean hasNext()
      {
        return objectSummaryIterator.hasNext() || result.isTruncated() || prefixes.hasNext();
      }

      @Override
      public InputSplit<URI> next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        if (objectSummaryIterator.hasNext()) {
          return new InputSplit<>(toUri(objectSummaryIterator.next()));
        }

        if (result.isTruncated()) {
          fetchNextBatch();
        } else if (prefixes.hasNext()) {
          prepareNextRequest();
          fetchNextBatch();
        }

        if (!objectSummaryIterator.hasNext()) {
          throw new ISE(
              "Failed to further iterate on bucket[%s] and prefix[%s]. The last continuationToken was [%s]",
              currentBucket,
              currentPrefix,
              result.getContinuationToken()
          );
        }

        return new InputSplit<>(toUri(objectSummaryIterator.next()));
      }
    };
  }
}
