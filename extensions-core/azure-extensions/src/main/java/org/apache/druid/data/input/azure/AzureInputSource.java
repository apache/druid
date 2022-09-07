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
import org.apache.druid.data.input.InputFileAttribute;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.CloudObjectInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.storage.azure.AzureCloudBlobHolderToCloudObjectLocationConverter;
import org.apache.druid.storage.azure.AzureCloudBlobIterableFactory;
import org.apache.druid.storage.azure.AzureInputDataConfig;
import org.apache.druid.storage.azure.AzureStorage;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;
import org.apache.druid.utils.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Abstracts the Azure storage system where input data is stored. Allows users to retrieve entities in
 * the storage system that match either a particular uri, prefix, or object.
 */
public class AzureInputSource extends CloudObjectInputSource
{
  public static final String SCHEME = "azure";

  private final AzureStorage storage;
  private final AzureEntityFactory entityFactory;
  private final AzureCloudBlobIterableFactory azureCloudBlobIterableFactory;
  private final AzureCloudBlobHolderToCloudObjectLocationConverter azureCloudBlobToLocationConverter;
  private final AzureInputDataConfig inputDataConfig;

  @JsonCreator
  public AzureInputSource(
      @JacksonInject AzureStorage storage,
      @JacksonInject AzureEntityFactory entityFactory,
      @JacksonInject AzureCloudBlobIterableFactory azureCloudBlobIterableFactory,
      @JacksonInject AzureCloudBlobHolderToCloudObjectLocationConverter azureCloudBlobToLocationConverter,
      @JacksonInject AzureInputDataConfig inputDataConfig,
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
    this.inputDataConfig = Preconditions.checkNotNull(inputDataConfig, "AzureInputDataConfig");
    this.azureCloudBlobToLocationConverter = Preconditions.checkNotNull(
        azureCloudBlobToLocationConverter,
        "AzureCloudBlobToLocationConverter"
    );
  }

  @Override
  public SplittableInputSource<List<CloudObjectLocation>> withSplit(InputSplit<List<CloudObjectLocation>> split)
  {
    return new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        azureCloudBlobToLocationConverter,
        inputDataConfig,
        null,
        null,
        split.get()
    );
  }

  @Override
  protected AzureEntity createEntity(CloudObjectLocation location)
  {
    return entityFactory.create(location);
  }

  @Override
  protected Stream<InputSplit<List<CloudObjectLocation>>> getPrefixesSplitStream(@Nonnull SplitHintSpec splitHintSpec)
  {
    final Iterator<List<CloudBlobHolder>> splitIterator = splitHintSpec.split(
        getIterableObjectsFromPrefixes().iterator(),
        blobHolder -> new InputFileAttribute(blobHolder.getBlobLength())
    );
    return Streams.sequentialStreamFrom(splitIterator)
                  .map(objects -> objects.stream()
                                         .map(azureCloudBlobToLocationConverter::createCloudObjectLocation)
                                         .collect(Collectors.toList()))
                  .map(InputSplit::new);
  }

  private Iterable<CloudBlobHolder> getIterableObjectsFromPrefixes()
  {
    return azureCloudBlobIterableFactory.create(getPrefixes(), inputDataConfig.getMaxListingLength());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        azureCloudBlobToLocationConverter,
        inputDataConfig
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
    AzureInputSource that = (AzureInputSource) o;
    return storage.equals(that.storage) &&
           entityFactory.equals(that.entityFactory) &&
           azureCloudBlobIterableFactory.equals(that.azureCloudBlobIterableFactory) &&
           azureCloudBlobToLocationConverter.equals(that.azureCloudBlobToLocationConverter) &&
           inputDataConfig.equals(that.inputDataConfig);
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
}
