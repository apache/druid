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

package org.apache.druid.data.input.google;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.services.storage.model.StorageObject;
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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.storage.google.GoogleByteSource;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class GoogleCloudStorageInputSource extends AbstractInputSource implements SplittableInputSource<GoogleByteSource>
{
  private final GoogleStorage storage;
  private final List<URI> uris;
  private final List<URI> prefixes;
  private final List<GoogleByteSource> byteSources;

  @JsonCreator
  public GoogleCloudStorageInputSource(
      @JacksonInject GoogleStorage storage,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("objects") @Nullable List<CloudObjectLocation> objects
  )
  {
    this.storage = storage;
    this.uris = uris == null ? new ArrayList<>() : uris;
    this.prefixes = prefixes == null ? new ArrayList<>() : prefixes;

    if (objects != null) {
      this.byteSources = objects.stream()
                                .map(x -> new GoogleByteSource(storage, x.getBucket(), x.getPath()))
                                .collect(Collectors.toList());
      if (!this.uris.isEmpty()) {
        throw new IAE("uris cannot be used with object");
      }
      if (!this.prefixes.isEmpty()) {
        throw new IAE("prefixes cannot be used with object");
      }
    } else {
      this.byteSources = null;
      if (!this.uris.isEmpty() && !this.prefixes.isEmpty()) {
        throw new IAE("uris and prefixes cannot be used together");
      }

      if (this.uris.isEmpty() && this.prefixes.isEmpty()) {
        throw new IAE("uris or prefixes must be specified");
      }

      for (final URI inputURI : this.uris) {
        Preconditions.checkArgument("gs".equals(inputURI.getScheme()), "input uri scheme == gs (%s)", inputURI);
      }

      for (final URI inputURI : this.prefixes) {
        Preconditions.checkArgument("gs".equals(inputURI.getScheme()), "input uri scheme == gs (%s)", inputURI);
      }
    }
  }

  private GoogleCloudStorageInputSource(
      GoogleStorage storage,
      GoogleByteSource byteSource
  )
  {
    this.storage = storage;
    this.byteSources = ImmutableList.of(byteSource);
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

  @Nullable
  @JsonProperty("objects")
  public List<CloudObjectLocation> getObjects()
  {
    if (byteSources != null) {
      return byteSources.stream()
                        .map(x -> new CloudObjectLocation(x.getBucket(), x.getPath()))
                        .collect(Collectors.toList());
    }
    return null;
  }

  @Override
  public Stream<InputSplit<GoogleByteSource>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    if (byteSources != null) {
      return byteSources.stream().map(InputSplit::new);
    }
    if (!uris.isEmpty()) {
      return uris.stream().map(this::byteSourceFromUri).map(InputSplit::new);
    }

    return StreamSupport.stream(storageObjectIterable().spliterator(), false)
                        .map(this::byteSourceFromStorageObject)
                        .map(InputSplit::new);
  }

  @Override
  public int getNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    if (byteSources != null) {
      return 1;
    }

    if (!uris.isEmpty()) {
      return uris.size();
    }

    return (int) StreamSupport.stream(storageObjectIterable().spliterator(), false).count();
  }

  @Override
  public SplittableInputSource<GoogleByteSource> withSplit(InputSplit<GoogleByteSource> split)
  {
    return new GoogleCloudStorageInputSource(storage, split.get());
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
        createSplits(inputFormat, null).map(split -> new GoogleCloudStorageEntity(split.get())),
        temporaryDirectory
    );
  }

  private GoogleByteSource byteSourceFromUri(final URI uri)
  {
    final String bucket = uri.getAuthority();
    final String path = GoogleUtils.extractGoogleCloudStorageObjectKey(uri);
    return new GoogleByteSource(storage, bucket, path);
  }

  private GoogleByteSource byteSourceFromStorageObject(final StorageObject storageObject)
  {
    return new GoogleByteSource(storage, storageObject.getBucket(), storageObject.getName());
  }

  private Iterable<StorageObject> storageObjectIterable()
  {
    return () ->
        GoogleUtils.lazyFetchingStorageObjectsIterator(storage, prefixes.iterator(), RetryUtils.DEFAULT_MAX_TRIES);
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
    GoogleCloudStorageInputSource that = (GoogleCloudStorageInputSource) o;
    return Objects.equals(uris, that.uris) &&
           Objects.equals(prefixes, that.prefixes) &&
           Objects.equals(byteSources, that.byteSources);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(uris, prefixes);
  }
}
