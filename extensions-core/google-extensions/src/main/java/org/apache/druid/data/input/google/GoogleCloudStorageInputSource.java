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
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.CloudConfigProperties;
import org.apache.druid.data.input.impl.CloudObjectInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleUtils;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class GoogleCloudStorageInputSource extends CloudObjectInputSource<GoogleCloudStorageEntity>
{
  static final String SCHEME = "gs";
  private static final int MAX_LISTING_LENGTH = 1024;

  private final GoogleStorage storage;

  @JsonCreator
  public GoogleCloudStorageInputSource(
      @JacksonInject GoogleStorage storage,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("objects") @Nullable List<CloudObjectLocation> objects,
      @JsonProperty("properties") @Nullable CloudConfigProperties cloudConfigProperties
  )
  {
    super(SCHEME, uris, prefixes, objects, cloudConfigProperties);
    this.storage = storage;
  }

  @Override
  protected GoogleCloudStorageEntity createEntity(InputSplit<CloudObjectLocation> split)
  {
    return new GoogleCloudStorageEntity(storage, split.get());
  }

  @Override
  protected Stream<InputSplit<CloudObjectLocation>> getPrefixesSplitStream()
  {
    return StreamSupport.stream(storageObjectIterable().spliterator(), false)
                        .map(this::byteSourceFromStorageObject)
                        .map(InputSplit::new);
  }

  @Override
  public SplittableInputSource<CloudObjectLocation> withSplit(InputSplit<CloudObjectLocation> split)
  {
    return new GoogleCloudStorageInputSource(
        storage,
        null,
        null,
        ImmutableList.of(split.get()),
        getCloudConfigProperties()
    );
  }

  private CloudObjectLocation byteSourceFromStorageObject(final StorageObject storageObject)
  {
    return new CloudObjectLocation(storageObject.getBucket(), storageObject.getName());
  }

  private Iterable<StorageObject> storageObjectIterable()
  {
    return () ->
        GoogleUtils.lazyFetchingStorageObjectsIterator(storage, getPrefixes().iterator(), MAX_LISTING_LENGTH);
  }

  @Override
  public String toString()
  {
    return "GoogleCloudStorageInputSource{" +
           "uris=" + getUris() +
           ", prefixes=" + getPrefixes() +
           ", objects=" + getObjects() +
           '}';
  }
}
