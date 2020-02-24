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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.CloudObjectInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.S3ConfigProperties;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.storage.s3.S3StorageConfig;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class S3InputSource extends CloudObjectInputSource<S3Entity>
{
  // We lazily initialize s3Client to avoid costly s3 operation when we only need S3InputSource for stored information
  // (such as for task logs) and not for ingestion. (This cost only applies for new s3Client created with
  // s3ConfigProperties given).
  private final Supplier<ServerSideEncryptingAmazonS3> s3Client;
  @JsonProperty("properties")
  private final S3ConfigProperties s3ConfigProperties;

  @JsonCreator
  public S3InputSource(
      @JacksonInject ServerSideEncryptingAmazonS3 s3Client,
      @JacksonInject AmazonS3ClientBuilder amazonS3ClientBuilder,
      @JacksonInject S3StorageConfig storageConfig,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("objects") @Nullable List<CloudObjectLocation> objects,
      @JsonProperty("properties") @Nullable S3ConfigProperties s3ConfigProperties
  )
  {
    super(S3StorageDruidModule.SCHEME, uris, prefixes, objects);
    this.s3ConfigProperties = s3ConfigProperties;
    this.s3Client = Suppliers.memoize(
        () -> {
          if (amazonS3ClientBuilder != null && storageConfig != null && s3ConfigProperties != null) {
            if (s3ConfigProperties.isCredentialsConfigured()) {
              BasicAWSCredentials creds = new BasicAWSCredentials(
                  s3ConfigProperties.getAccessKeyId().getPassword(),
                  s3ConfigProperties.getSecretAccessKey().getPassword());
              amazonS3ClientBuilder.withCredentials(new AWSStaticCredentialsProvider(creds));
            }
            return new ServerSideEncryptingAmazonS3(amazonS3ClientBuilder.build(), storageConfig.getServerSideEncryption());
          } else {
            return Preconditions.checkNotNull(s3Client, "s3Client");
          }
        }
    );
  }

  @Nullable
  @JsonProperty("properties")
  public S3ConfigProperties getS3ConfigProperties()
  {
    return s3ConfigProperties;
  }

  @Override
  protected S3Entity createEntity(InputSplit<CloudObjectLocation> split)
  {
    return new S3Entity(s3Client.get(), split.get());
  }

  @Override
  protected Stream<InputSplit<CloudObjectLocation>> getPrefixesSplitStream()
  {
    return StreamSupport.stream(getIterableObjectsFromPrefixes().spliterator(), false)
                        .map(S3Utils::summaryToCloudObjectLocation)
                        .map(InputSplit::new);
  }

  @Override
  public SplittableInputSource<CloudObjectLocation> withSplit(InputSplit<CloudObjectLocation> split)
  {
    return new S3InputSource(
        s3Client.get(),
       null,
       null,
       null,
       null,
       ImmutableList.of(split.get()),
       getS3ConfigProperties()
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        s3ConfigProperties
    );
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
    if (!super.equals(o)) {
      return false;
    }
    S3InputSource that = (S3InputSource) o;
    return Objects.equals(s3ConfigProperties, that.s3ConfigProperties);
  }

  @Override
  public String toString()
  {
    return "S3InputSource{" +
           "uris=" + getUris() +
           ", prefixes=" + getPrefixes() +
           ", objects=" + getObjects() +
           ", s3ConfigProperties=" + getS3ConfigProperties() +
           '}';
  }

  private Iterable<S3ObjectSummary> getIterableObjectsFromPrefixes()
  {
    return () -> S3Utils.objectSummaryIterator(s3Client.get(), getPrefixes(), MAX_LISTING_LENGTH);
  }
}
