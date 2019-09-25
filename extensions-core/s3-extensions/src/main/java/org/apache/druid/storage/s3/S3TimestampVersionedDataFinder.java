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

package org.apache.druid.storage.s3;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.inject.Inject;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Iterator;
import java.util.regex.Pattern;

public class S3TimestampVersionedDataFinder extends S3DataSegmentPuller implements SearchableVersionedDataFinder<URI>
{
  private static final int MAX_LISTING_KEYS = 1000;

  @Inject
  public S3TimestampVersionedDataFinder(ServerSideEncryptingAmazonS3 s3Client)
  {
    super(s3Client);
  }

  /**
   * Gets the key with the most recently modified timestamp.
   * `pattern` is evaluated against the entire key AFTER the path given in `uri`.
   * The substring `pattern` is matched against will have a leading `/` removed.
   * For example `s3://some_bucket/some_prefix/some_key` with a URI of `s3://some_bucket/some_prefix` will match against `some_key`.
   * `s3://some_bucket/some_prefixsome_key` with a URI of `s3://some_bucket/some_prefix` will match against `some_key`
   * `s3://some_bucket/some_prefix//some_key` with a URI of `s3://some_bucket/some_prefix` will match against `/some_key`
   *
   * @param uri     The URI of in the form of `s3://some_bucket/some_key`
   * @param pattern The pattern matcher to determine if a *key* is of interest, or `null` to match everything.
   *
   * @return A URI to the most recently modified object which matched the pattern.
   */
  @Override
  public URI getLatestVersion(final URI uri, final @Nullable Pattern pattern)
  {
    try {
      return RetryUtils.retry(
          () -> {
            final S3Coords coords = new S3Coords(checkURI(uri));
            long mostRecent = Long.MIN_VALUE;
            URI latest = null;
            final Iterator<S3ObjectSummary> objectSummaryIterator = S3Utils.objectSummaryIterator(
                s3Client,
                coords.bucket,
                coords.path,
                MAX_LISTING_KEYS
            );
            while (objectSummaryIterator.hasNext()) {
              final S3ObjectSummary objectSummary = objectSummaryIterator.next();
              String keyString = objectSummary.getKey().substring(coords.path.length());
              if (keyString.startsWith("/")) {
                keyString = keyString.substring(1);
              }
              if (pattern != null && !pattern.matcher(keyString).matches()) {
                continue;
              }
              final long latestModified = objectSummary.getLastModified().getTime();
              if (latestModified >= mostRecent) {
                mostRecent = latestModified;
                latest = new URI(
                    StringUtils.format("s3://%s/%s", objectSummary.getBucketName(), objectSummary.getKey())
                );
              }
            }
            return latest;
          },
          shouldRetryPredicate(),
          DEFAULT_RETRY_COUNT
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
