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
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class S3InputSource extends AbstractInputSource implements SplittableInputSource<CloudObjectLocation>
{
  private static final int MAX_LISTING_LENGTH = 1024;

  private final ServerSideEncryptingAmazonS3 s3Client;
  private final List<URI> uris;
  private final List<URI> prefixes;
  private final List<CloudObjectLocation> objects;

  @JsonCreator
  public S3InputSource(
      @JacksonInject ServerSideEncryptingAmazonS3 s3Client,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("objects") @Nullable List<CloudObjectLocation> objects
  )
  {
    this.s3Client = Preconditions.checkNotNull(s3Client, "s3Client");
    this.uris = uris;
    this.prefixes = prefixes;
    this.objects = objects;

    if (!CollectionUtils.isNullOrEmpty(objects)) {
      throwIfIllegalArgs(!CollectionUtils.isNullOrEmpty(uris) || !CollectionUtils.isNullOrEmpty(prefixes));
    } else if (!CollectionUtils.isNullOrEmpty(uris)) {
      throwIfIllegalArgs(!CollectionUtils.isNullOrEmpty(prefixes));
      uris.forEach(S3Utils::checkURI);
    } else if (!CollectionUtils.isNullOrEmpty(prefixes)) {
      prefixes.forEach(S3Utils::checkURI);
    } else {
      throwIfIllegalArgs(true);
    }
  }

  @JsonProperty
  public List<URI> getUris()
  {
    return uris;
  }

  @JsonProperty
  public List<URI> getPrefixes()
  {
    return prefixes;
  }

  @JsonProperty
  public List<CloudObjectLocation> getObjects()
  {
    return objects;
  }

  @Override
  public Stream<InputSplit<CloudObjectLocation>> createSplits(
      InputFormat inputFormat,
      @Nullable SplitHintSpec splitHintSpec
  )
  {
    if (objects != null) {
      return objects.stream().map(InputSplit::new);
    }

    if (uris != null) {
      return uris.stream().map(CloudObjectLocation::new).map(InputSplit::new);
    }

    return StreamSupport.stream(getIterableObjectsFromPrefixes().spliterator(), false)
                        .map(S3Utils::summaryToCloudObjectLocation)
                        .map(InputSplit::new);
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    if (objects != null) {
      return objects.size();
    }

    if (uris != null) {
      return uris.size();
    }

    return (int) StreamSupport.stream(getIterableObjectsFromPrefixes().spliterator(), false).count();
  }

  @Override
  public SplittableInputSource<CloudObjectLocation> withSplit(InputSplit<CloudObjectLocation> split)
  {
    return new S3InputSource(s3Client, null, null, ImmutableList.of(split.get()));
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
           Objects.equals(prefixes, that.prefixes) &&
           Objects.equals(objects, that.objects);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(uris, prefixes, objects);
  }

  @Override
  public String toString()
  {
    return "S3InputSource{" +
           "uris=" + uris +
           ", prefixes=" + prefixes +
           ", objects=" + objects +
           '}';
  }

  private Iterable<S3ObjectSummary> getIterableObjectsFromPrefixes()
  {
    return () -> S3Utils.lazyFetchingObjectSummariesIterator(s3Client, prefixes.iterator(), MAX_LISTING_LENGTH);
  }

  private void throwIfIllegalArgs(boolean clause) throws IllegalArgumentException
  {
    if (clause) {
      throw new IllegalArgumentException("exactly one of either uris or prefixes or objects must be specified");
    }
  }
}
