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
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFileAttribute;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.CloudObjectInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleUtils;
import org.apache.druid.utils.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GoogleCloudStorageInputSource extends CloudObjectInputSource
{
  public static final String SCHEME = "gs";

  private static final Logger LOG = new Logger(GoogleCloudStorageInputSource.class);

  private final GoogleStorage storage;
  private final GoogleInputDataConfig inputDataConfig;

  @JsonCreator
  public GoogleCloudStorageInputSource(
      @JacksonInject GoogleStorage storage,
      @JacksonInject GoogleInputDataConfig inputDataConfig,
      @JsonProperty("uris") @Nullable List<URI> uris,
      @JsonProperty("prefixes") @Nullable List<URI> prefixes,
      @JsonProperty("objects") @Nullable List<CloudObjectLocation> objects
  )
  {
    super(SCHEME, uris, prefixes, objects);
    this.storage = storage;
    this.inputDataConfig = inputDataConfig;
  }

  @Override
  protected InputEntity createEntity(CloudObjectLocation location)
  {
    return new GoogleCloudStorageEntity(storage, location);
  }

  @Override
  protected Stream<InputSplit<List<CloudObjectLocation>>> getPrefixesSplitStream(@Nonnull SplitHintSpec splitHintSpec)
  {
    final Iterator<List<StorageObject>> splitIterator = splitHintSpec.split(
        storageObjectIterable().iterator(),
        storageObject -> {
          final BigInteger sizeInBigInteger = storageObject.getSize();
          long sizeInLong;
          if (sizeInBigInteger == null) {
            sizeInLong = Long.MAX_VALUE;
          } else {
            try {
              sizeInLong = sizeInBigInteger.longValueExact();
            }
            catch (ArithmeticException e) {
              LOG.warn(
                  e,
                  "The object [%s, %s] has a size [%s] out of the range of the long type. "
                  + "The max long value will be used for its size instead.",
                  storageObject.getBucket(),
                  storageObject.getName(),
                  sizeInBigInteger
              );
              sizeInLong = Long.MAX_VALUE;
            }
          }
          return new InputFileAttribute(sizeInLong);
        }
    );

    return Streams.sequentialStreamFrom(splitIterator)
                  .map(objects -> objects.stream().map(this::byteSourceFromStorageObject).collect(Collectors.toList()))
                  .map(InputSplit::new);
  }

  @Override
  public SplittableInputSource<List<CloudObjectLocation>> withSplit(InputSplit<List<CloudObjectLocation>> split)
  {
    return new GoogleCloudStorageInputSource(storage, inputDataConfig, null, null, split.get());
  }

  private CloudObjectLocation byteSourceFromStorageObject(final StorageObject storageObject)
  {
    return GoogleUtils.objectToCloudObjectLocation(storageObject);
  }

  private Iterable<StorageObject> storageObjectIterable()
  {
    return () ->
        GoogleUtils.lazyFetchingStorageObjectsIterator(
            storage,
            getPrefixes().iterator(),
            inputDataConfig.getMaxListingLength()
        );
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
