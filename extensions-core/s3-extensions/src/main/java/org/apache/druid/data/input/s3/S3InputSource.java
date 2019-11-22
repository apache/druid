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
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.storage.s3.S3Coords;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class S3InputSource extends AbstractInputSource implements SplittableInputSource<S3Coords>
{
  private static final int MAX_LISTING_LENGTH = 1024;

  private final ServerSideEncryptingAmazonS3 s3Client;
  private final List<URI> uris;
  private final List<URI> prefixes;
  private final S3Coords object;

  @JsonCreator
  public S3InputSource(
      @JacksonInject ServerSideEncryptingAmazonS3 s3Client,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("object") @Nullable S3Coords object
  )
  {
    this.s3Client = Preconditions.checkNotNull(s3Client, "s3Client");
    this.uris = uris == null ? new ArrayList<>() : uris;
    this.prefixes = prefixes == null ? new ArrayList<>() : prefixes;

    if (object != null) {
      this.object = object;
      if (!this.uris.isEmpty()) {
        throw new IAE("uris cannot be used with object");
      }
      if (!this.prefixes.isEmpty()) {
        throw new IAE("prefixes cannot be used with object");
      }
    } else {
      this.object = null;
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
  }

  private S3InputSource(ServerSideEncryptingAmazonS3 s3Client, S3Coords inputSplit)
  {
    this.s3Client = Preconditions.checkNotNull(s3Client, "s3Client");
    this.object = Preconditions.checkNotNull(inputSplit, "object");
    this.uris = new ArrayList<>();
    this.prefixes = new ArrayList<>();
  }

  @JsonProperty("uris")
  public List<URI> getUris()
  {
    return uris;
  }

  @JsonProperty("prefixes")
  public List<URI> getPrefixes()
  {
    return prefixes;
  }

  @JsonProperty("object")
  public S3Coords getObject()
  {
    return object;
  }

  @Override
  public Stream<InputSplit<S3Coords>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    if (object != null) {
      return Stream.of(new InputSplit<>(object));
    }

    if (!uris.isEmpty()) {
      return uris.stream().map(S3Coords::new).map(InputSplit::new);
    }

    return StreamSupport.stream(getIterableObjectsFromPrefixes().spliterator(), false)
                        .map(S3Utils::summaryToS3Coords)
                        .map(InputSplit::new);
  }

  @Override
  public int getNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    if (object != null) {
      return 1;
    }

    if (!uris.isEmpty()) {
      return uris.size();
    }

    return (int) StreamSupport.stream(getIterableObjectsFromPrefixes().spliterator(), false).count();
  }

  @Override
  public SplittableInputSource<S3Coords> withSplit(InputSplit<S3Coords> split)
  {
    return new S3InputSource(s3Client, split.get());
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
           Objects.equals(object, that.object);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(uris, prefixes, object);
  }

  @Override
  public String toString()
  {
    return "S3InputSource{" +
           "uris=" + uris +
           ", prefixes=" + prefixes +
           ", object=" + object +
           '}';
  }

  private Iterable<S3ObjectSummary> getIterableObjectsFromPrefixes()
  {
    return () -> S3Utils.lazyFetchingObjectSummariesIterator(s3Client, prefixes.iterator(), MAX_LISTING_LENGTH);
  }
}
