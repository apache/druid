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

import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.CloudObjectInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.CloudObjectSplitWidget;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.storage.azure.AzureCloudBlobIterableFactory;
import org.apache.druid.storage.azure.AzureInputDataConfig;
import org.apache.druid.storage.azure.AzureStorage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
  private final AzureInputDataConfig inputDataConfig;

  @JsonCreator
  public AzureInputSource(
      @JacksonInject @Global AzureStorage storage,
      @JacksonInject AzureEntityFactory entityFactory,
      @JacksonInject AzureCloudBlobIterableFactory azureCloudBlobIterableFactory,
      @JacksonInject AzureInputDataConfig inputDataConfig,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("objects") @Nullable List<CloudObjectLocation> objects,
      @JsonProperty("objectGlob") @Nullable String objectGlob,
      @JsonProperty(SYSTEM_FIELDS_PROPERTY) @Nullable SystemFields systemFields
  )
  {
    super(SCHEME, uris, prefixes, objects, objectGlob, systemFields);
    this.storage = Preconditions.checkNotNull(storage, "AzureStorage");
    this.entityFactory = Preconditions.checkNotNull(entityFactory, "AzureEntityFactory");
    this.azureCloudBlobIterableFactory = Preconditions.checkNotNull(
        azureCloudBlobIterableFactory,
        "AzureCloudBlobIterableFactory"
    );
    this.inputDataConfig = Preconditions.checkNotNull(inputDataConfig, "AzureInputDataConfig");
  }

  @JsonIgnore
  @Nonnull
  @Override
  public Set<String> getTypes()
  {
    return Collections.singleton(SCHEME);
  }

  @Override
  public SplittableInputSource<List<CloudObjectLocation>> withSplit(InputSplit<List<CloudObjectLocation>> split)
  {
    return new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        null,
        null,
        split.get(),
        getObjectGlob(),
        systemFields
    );
  }

  @Override
  public Object getSystemFieldValue(InputEntity entity, SystemField field)
  {
    final AzureEntity googleEntity = (AzureEntity) entity;

    switch (field) {
      case URI:
        return googleEntity.getUri().toString();
      case BUCKET:
        return googleEntity.getLocation().getBucket();
      case PATH:
        return googleEntity.getLocation().getPath();
      default:
        return null;
    }
  }

  @Override
  protected AzureEntity createEntity(CloudObjectLocation location)
  {
    return entityFactory.create(location, storage, SCHEME);
  }

  @Override
  protected CloudObjectSplitWidget getSplitWidget()
  {
    class SplitWidget implements CloudObjectSplitWidget
    {
      @Override
      public Iterator<LocationWithSize> getDescriptorIteratorForPrefixes(List<URI> prefixes)
      {
        return Iterators.transform(
            azureCloudBlobIterableFactory.create(getPrefixes(), inputDataConfig.getMaxListingLength(), storage).iterator(),
            blob -> {
              try {
                return new LocationWithSize(
                    blob.getContainerName(),
                    blob.getName(),
                    blob.getBlobLength()
                );
              }
              catch (BlobStorageException e) {
                throw new RuntimeException(e);
              }
            }
        );
      }

      @Override
      public long getObjectSize(CloudObjectLocation location)
      {
        try {
          final BlockBlobClient blobWithAttributes = storage.getBlockBlobReferenceWithAttributes(
              location.getBucket(),
              location.getPath()
          );

          return blobWithAttributes.getProperties().getBlobSize();
        }
        catch (BlobStorageException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return new SplitWidget();
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
           inputDataConfig.equals(that.inputDataConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), storage, entityFactory, azureCloudBlobIterableFactory, inputDataConfig);
  }

  @Override
  public String toString()
  {
    return "AzureInputSource{" +
           "uris=" + getUris() +
           ", prefixes=" + getPrefixes() +
           ", objects=" + getObjects() +
           ", objectGlob=" + getObjectGlob() +
           (systemFields.getFields().isEmpty() ? "" : ", systemFields=" + systemFields) +
           '}';
  }
}
