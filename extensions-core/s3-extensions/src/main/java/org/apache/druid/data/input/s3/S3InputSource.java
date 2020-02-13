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

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.CloudObjectInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.storage.s3.S3DataSegmentPusherConfig;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class S3InputSource extends CloudObjectInputSource<S3Entity>
{
  private final ServerSideEncryptingAmazonS3 s3Client;
  private final S3DataSegmentPusherConfig segmentPusherConfig;

  @JsonCreator
  public S3InputSource(
      @JacksonInject ServerSideEncryptingAmazonS3 s3Client,
      @JacksonInject S3DataSegmentPusherConfig segmentPusherConfig,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("objects") @Nullable List<CloudObjectLocation> objects
  )
  {
    super(S3StorageDruidModule.SCHEME, uris, prefixes, objects);
    this.s3Client = Preconditions.checkNotNull(s3Client, "s3Client");
    this.segmentPusherConfig = Preconditions.checkNotNull(segmentPusherConfig, "S3DataSegmentPusherConfig");
  }

  @Override
  protected S3Entity createEntity(InputSplit<CloudObjectLocation> split)
  {
    return new S3Entity(s3Client, split.get());
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
    return new S3InputSource(s3Client, segmentPusherConfig, null, null, ImmutableList.of(split.get()));
  }

  @Override
  public String toString()
  {
    return "S3InputSource{" +
           "uris=" + getUris() +
           ", prefixes=" + getPrefixes() +
           ", objects=" + getObjects() +
           '}';
  }

  private Iterable<S3ObjectSummary> getIterableObjectsFromPrefixes()
  {
    return () -> S3Utils.objectSummaryIterator(s3Client, getPrefixes(), segmentPusherConfig.getMaxListingLength());
  }
}
