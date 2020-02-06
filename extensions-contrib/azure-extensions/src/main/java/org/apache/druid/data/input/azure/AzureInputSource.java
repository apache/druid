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

package org.apache.druid.data.input.azure;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.CloudObjectInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.storage.azure.AzureCloudBlobHolderToCloudObjectLocationConverter;
import org.apache.druid.storage.azure.AzureCloudBlobIterableFactory;
import org.apache.druid.storage.azure.AzureStorage;
import org.apache.druid.storage.azure.CloudBlobHolder;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Abstracts the Azure storage system where input data is stored. Allows users to retrieve entities in
 * the storage system that match either a particular uri, prefix, or object.
 */
public class AzureInputSource extends CloudObjectInputSource<AzureEntity>
{
  static final int MAX_LISTING_LENGTH = 1024;
  public static final String SCHEME = "azure";

  private final AzureStorage storage;
  private final AzureEntityFactory entityFactory;
  private final AzureCloudBlobIterableFactory azureCloudBlobIterableFactory;
  private final AzureCloudBlobHolderToCloudObjectLocationConverter azureCloudBlobToLocationConverter;

  @JsonCreator
  public AzureInputSource(
      @JacksonInject AzureStorage storage,
      @JacksonInject AzureEntityFactory entityFactory,
      @JacksonInject AzureCloudBlobIterableFactory azureCloudBlobIterableFactory,
      @JacksonInject AzureCloudBlobHolderToCloudObjectLocationConverter azureCloudBlobToLocationConverter,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("objects") @Nullable List<CloudObjectLocation> objects
  )
  {
    super(SCHEME, uris, prefixes, objects);
    this.storage = Preconditions.checkNotNull(storage, "AzureStorage");
    this.entityFactory = Preconditions.checkNotNull(entityFactory, "AzureEntityFactory");
    this.azureCloudBlobIterableFactory = Preconditions.checkNotNull(
        azureCloudBlobIterableFactory,
        "AzureCloudBlobIterableFactory"
    );
    this.azureCloudBlobToLocationConverter = Preconditions.checkNotNull(azureCloudBlobToLocationConverter, "AzureCloudBlobToLocationConverter");
  }

  @Override
  public SplittableInputSource<CloudObjectLocation> withSplit(InputSplit<CloudObjectLocation> split)
  {
    return new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        azureCloudBlobToLocationConverter,
        null,
        null,
        ImmutableList.of(split.get())
    );
  }

  @Override
  public String toString()
  {
    return "AzureInputSource{" +
           "uris=" + getUris() +
           ", prefixes=" + getPrefixes() +
           ", objects=" + getObjects() +
           '}';
  }

  @Override
  protected AzureEntity createEntity(InputSplit<CloudObjectLocation> split)
  {
    return entityFactory.create(split.get());
  }

  @Override
  protected Stream<InputSplit<CloudObjectLocation>> getPrefixesSplitStream()
  {
    return StreamSupport.stream(getIterableObjectsFromPrefixes().spliterator(), false)
                        .map(o -> azureCloudBlobToLocationConverter.createCloudObjectLocation(o))
                        .map(InputSplit::new);
  }

  private Iterable<CloudBlobHolder> getIterableObjectsFromPrefixes()
  {
    return azureCloudBlobIterableFactory.create(getPrefixes(), MAX_LISTING_LENGTH);
  }
}
