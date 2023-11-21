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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterators;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.CloudObjectInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.CloudObjectSplitWidget;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleStorageDruidModule;
import org.apache.druid.storage.google.GoogleStorageObjectMetadata;
import org.apache.druid.storage.google.GoogleUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class GoogleCloudStorageInputSource extends CloudObjectInputSource
{
  static final String TYPE_KEY = GoogleStorageDruidModule.SCHEME;
  private static final Logger LOG = new Logger(GoogleCloudStorageInputSource.class);

  private final GoogleStorage storage;
  private final GoogleInputDataConfig inputDataConfig;

  @JsonCreator
  public GoogleCloudStorageInputSource(
      @JacksonInject GoogleStorage storage,
      @JacksonInject GoogleInputDataConfig inputDataConfig,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("objects") @Nullable List<CloudObjectLocation> objects,
      @JsonProperty("objectGlob") @Nullable String objectGlob,
      @JsonProperty(SYSTEM_FIELDS_PROPERTY) @Nullable SystemFields systemFields
  )
  {
    super(GoogleStorageDruidModule.SCHEME_GS, uris, prefixes, objects, objectGlob, systemFields);
    this.storage = storage;
    this.inputDataConfig = inputDataConfig;
  }

  @JsonIgnore
  @Nonnull
  @Override
  public Set<String> getTypes()
  {
    return Collections.singleton(TYPE_KEY);
  }

  @Override
  protected InputEntity createEntity(CloudObjectLocation location)
  {
    return new GoogleCloudStorageEntity(storage, location);
  }

  @Override
  public SplittableInputSource<List<CloudObjectLocation>> withSplit(InputSplit<List<CloudObjectLocation>> split)
  {
    return new GoogleCloudStorageInputSource(
        storage,
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
    final GoogleCloudStorageEntity googleEntity = (GoogleCloudStorageEntity) entity;

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
  protected CloudObjectSplitWidget getSplitWidget()
  {
    class SplitWidget implements CloudObjectSplitWidget
    {
      @Override
      public Iterator<LocationWithSize> getDescriptorIteratorForPrefixes(List<URI> prefixes)
      {
        return Iterators.transform(
            GoogleUtils.lazyFetchingStorageObjectsIterator(
                storage,
                prefixes.iterator(),
                inputDataConfig.getMaxListingLength()
            ),
            object -> new LocationWithSize(object.getBucket(), object.getName(), getSize(object))
        );
      }

      @Override
      public long getObjectSize(CloudObjectLocation location) throws IOException
      {
        final GoogleStorageObjectMetadata storageObject = storage.getMetadata(location.getBucket(), location.getPath());
        return getSize(storageObject);
      }
    }

    return new SplitWidget();
  }

  private static long getSize(final GoogleStorageObjectMetadata object)
  {
    final Long sizeInLong = object.getSize();

    if (sizeInLong == null) {
      return Long.MAX_VALUE;
    } else {
      try {
        return sizeInLong;
      }
      catch (ArithmeticException e) {
        LOG.warn(
            e,
            "The object [%s, %s] has a size [%s] out of the range of the long type. "
            + "The max long value will be used for its size instead.",
            object.getBucket(),
            object.getName(),
            sizeInLong
        );
        return Long.MAX_VALUE;
      }
    }
  }

  @Override
  public String toString()
  {
    return "GoogleCloudStorageInputSource{" +
           "uris=" + getUris() +
           ", prefixes=" + getPrefixes() +
           ", objects=" + getObjects() +
           ", objectGlob=" + getObjectGlob() +
           (systemFields.getFields().isEmpty() ? "" : ", systemFields=" + systemFields) +
           '}';
  }
}
