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
import com.aliyun.oss.model.OSSObjectSummary;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFileAttribute;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.CloudObjectInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.storage.aliyun.OssInputDataConfig;
import org.apache.druid.storage.aliyun.OssStorageDruidModule;
import org.apache.druid.storage.aliyun.OssUtils;
import org.apache.druid.utils.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
      @JsonProperty("properties") @Nullable OssClientConfig inputSourceConfig
  )
  {
    super(OssStorageDruidModule.SCHEME, uris, prefixes, objects);
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
  protected Stream<InputSplit<List<CloudObjectLocation>>> getPrefixesSplitStream(@Nonnull SplitHintSpec splitHintSpec)
  {
    final Iterator<List<OSSObjectSummary>> splitIterator = splitHintSpec.split(
        getIterableObjectsFromPrefixes().iterator(),
        object -> new InputFileAttribute(object.getSize())
    );

    return Streams.sequentialStreamFrom(splitIterator)
                  .map(objects -> objects.stream()
                                         .map(OssUtils::summaryToCloudObjectLocation)
                                         .collect(Collectors.toList()))
                  .map(InputSplit::new);
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
        getOssInputSourceConfig()
    );
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
           ", ossInputSourceConfig=" + getOssInputSourceConfig() +
           '}';
  }

  private Iterable<OSSObjectSummary> getIterableObjectsFromPrefixes()
  {
    return () -> OssUtils.objectSummaryIterator(
        clientSupplier.get(),
        getPrefixes(),
        inputDataConfig.getMaxListingLength()
    );
  }
}
