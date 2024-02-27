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

package org.apache.druid.data.input.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ObjectMetadata;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.CloudObjectInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.CloudObjectSplitWidget;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.storage.aliyun.OssInputDataConfig;
import org.apache.druid.storage.aliyun.OssStorageDruidModule;
import org.apache.druid.storage.aliyun.OssUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class OssInputSource extends CloudObjectInputSource
{
  private final Supplier<OSS> clientSupplier;
  @JsonProperty("properties")
  private final OssClientConfig inputSourceConfig;
  private final OssInputDataConfig inputDataConfig;

  /**
   * Constructor for OssInputSource
   *
   * @param client            The default client built with all default configs
   *                          from Guice. This injected singleton client is used when {@param inputSourceConfig}
   *                          is not provided and hence
   * @param inputDataConfig   Stores the configuration for options related to reading input data
   * @param uris              User provided uris to read input data
   * @param prefixes          User provided prefixes to read input data
   * @param objects           User provided cloud objects values to read input data
   * @param inputSourceConfig User provided properties for overriding the default aliyun OSS configuration
   */
  @JsonCreator
  public OssInputSource(
      @JacksonInject OSS client,
      @JacksonInject OssInputDataConfig inputDataConfig,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("objects") @Nullable List<CloudObjectLocation> objects,
      @JsonProperty("objectGlob") @Nullable String objectGlob,
      @JsonProperty(SYSTEM_FIELDS_PROPERTY) @Nullable SystemFields systemFields,
      @JsonProperty("properties") @Nullable OssClientConfig inputSourceConfig
  )
  {
    super(OssStorageDruidModule.SCHEME, uris, prefixes, objects, objectGlob, systemFields);
    this.inputDataConfig = Preconditions.checkNotNull(inputDataConfig, "inputDataConfig");
    Preconditions.checkNotNull(client, "client");
    this.inputSourceConfig = inputSourceConfig;
    this.clientSupplier = Suppliers.memoize(
        () -> {
          if (inputSourceConfig != null) {
            return inputSourceConfig.buildClient();
          } else {
            return client;
          }
        }
    );
  }

  @Override
  @JsonIgnore
  @Nonnull
  public Set<String> getTypes()
  {
    return ImmutableSet.of(OssStorageDruidModule.SCHEME);
  }


  @Nullable
  @JsonProperty("properties")
  public OssClientConfig getOssInputSourceConfig()
  {
    return inputSourceConfig;
  }

  @Override
  protected InputEntity createEntity(CloudObjectLocation location)
  {
    return new OssEntity(clientSupplier.get(), location);
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
            OssUtils.objectSummaryIterator(
                clientSupplier.get(),
                getPrefixes(),
                inputDataConfig.getMaxListingLength()
            ),
            object -> new LocationWithSize(object.getBucketName(), object.getKey(), object.getSize())
        );
      }

      @Override
      public long getObjectSize(CloudObjectLocation location)
      {
        final ObjectMetadata objectMetadata = OssUtils.getSingleObjectMetadata(
            clientSupplier.get(),
            location.getBucket(),
            location.getPath()
        );

        return objectMetadata.getContentLength();
      }
    }

    return new SplitWidget();
  }

  @Override
  public SplittableInputSource<List<CloudObjectLocation>> withSplit(InputSplit<List<CloudObjectLocation>> split)
  {
    return new OssInputSource(
        clientSupplier.get(),
        inputDataConfig,
        null,
        null,
        split.get(),
        getObjectGlob(),
        systemFields,
        getOssInputSourceConfig()
    );
  }

  @Override
  public Object getSystemFieldValue(InputEntity entity, SystemField field)
  {
    final OssEntity s3Entity = (OssEntity) entity;

    switch (field) {
      case URI:
        return s3Entity.getUri().toString();
      case BUCKET:
        return s3Entity.getObject().getBucket();
      case PATH:
        return s3Entity.getObject().getPath();
      default:
        return null;
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), inputSourceConfig);
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
    OssInputSource that = (OssInputSource) o;
    return Objects.equals(inputSourceConfig, that.inputSourceConfig);
  }

  @Override
  public String toString()
  {
    return "OssInputSource{" +
           "uris=" + getUris() +
           ", prefixes=" + getPrefixes() +
           ", objects=" + getObjects() +
           ", objectGlob=" + getObjectGlob() +
           (systemFields.getFields().isEmpty() ? "" : ", systemFields=" + systemFields) +
           ", ossInputSourceConfig=" + getOssInputSourceConfig() +
           '}';
  }
}
