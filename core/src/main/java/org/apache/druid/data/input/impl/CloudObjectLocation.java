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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.net.URI;
import java.util.Objects;

/**
 * Common type for 'bucket' and 'path' concept of cloud objects to allow code sharing between cloud specific
 * implementations. {@link #bucket} and {@link #path} should NOT be URL encoded.
 *
 * The intention is that this is used as a common representation for storage objects as an alternative to dealing in
 * {@link URI} directly, but still provide a mechanism to round-trip with a URI.
 *
 * In common clouds, bucket names must be dns compliant:
 * https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
 * https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
 * https://cloud.google.com/storage/docs/naming
 *
 * The constructor ensures that bucket names are DNS compliant by checking that the URL encoded form of the bucket
 * matches the supplied value. Technically it should probably confirm that the bucket is also all lower-case, but
 * S3 has a legacy mode where buckets did not have to be compliant so we can't enforce that here unfortunately.
 */
public class CloudObjectLocation
{
  public static URI validateUriScheme(String scheme, URI uri)
  {
    if (!scheme.equalsIgnoreCase(uri.getScheme())) {
      throw new IAE("Invalid URI scheme [%s] must be [%s]", uri.toString(), scheme);
    }
    return uri;
  }

  private final String bucket;
  private final String path;

  @JsonCreator
  public CloudObjectLocation(@JsonProperty("bucket") String bucket, @JsonProperty("path") String path)
  {
    this.bucket = Preconditions.checkNotNull(StringUtils.maybeRemoveTrailingSlash(bucket));
    this.path = Preconditions.checkNotNull(StringUtils.maybeRemoveLeadingSlash(path));
    Preconditions.checkArgument(
        this.bucket.equals(StringUtils.urlEncode(this.bucket)),
        "bucket must follow DNS-compliant naming conventions"
    );
  }

  public CloudObjectLocation(URI uri)
  {
    this(uri.getHost(), uri.getPath());
  }

  /**
   * Given a scheme, encode {@link #bucket} and {@link #path} into a {@link URI}.
   *
   * In all clouds bucket names must be dns compliant, so it does not require encoding
   * There is no such restriction on object names, so they will be URL encoded when constructing the URI
   */
  public URI toUri(String scheme)
  {
    // Encode path, except leave '/' characters unencoded
    return URI.create(
        StringUtils.format(
            "%s://%s/%s",
            scheme,
            bucket,
            StringUtils.replace(StringUtils.urlEncode(path), "%2F", "/")
        )
    );
  }

  @JsonProperty
  public String getBucket()
  {
    return bucket;
  }

  @JsonProperty
  public String getPath()
  {
    return path;
  }

  @Override
  public String toString()
  {
    return "CloudObjectLocation{" +
           "bucket='" + bucket + '\'' +
           ", path='" + path + '\'' +
           '}';
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

    final CloudObjectLocation that = (CloudObjectLocation) o;
    return Objects.equals(bucket, that.bucket) &&
           Objects.equals(path, that.path);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucket, path);
  }
}
