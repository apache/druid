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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Ints;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public abstract class CloudObjectInputSource extends AbstractInputSource
    implements SplittableInputSource<List<CloudObjectLocation>>
{
  private final List<URI> uris;
  private final List<URI> prefixes;
  private final List<CloudObjectLocation> objects;

  public CloudObjectInputSource(
      String scheme,
      @Nullable List<URI> uris,
      @Nullable List<URI> prefixes,
      @Nullable List<CloudObjectLocation> objects
  )
  {
    this.uris = uris;
    this.prefixes = prefixes;
    this.objects = objects;

    if (!CollectionUtils.isNullOrEmpty(objects)) {
      throwIfIllegalArgs(!CollectionUtils.isNullOrEmpty(uris) || !CollectionUtils.isNullOrEmpty(prefixes));
    } else if (!CollectionUtils.isNullOrEmpty(uris)) {
      throwIfIllegalArgs(!CollectionUtils.isNullOrEmpty(prefixes));
      uris.forEach(uri -> CloudObjectLocation.validateUriScheme(scheme, uri));
    } else if (!CollectionUtils.isNullOrEmpty(prefixes)) {
      prefixes.forEach(uri -> CloudObjectLocation.validateUriScheme(scheme, uri));
    } else {
      throwIfIllegalArgs(true);
    }
  }

  @JsonProperty
  public List<URI> getUris()
  {
    return uris;
  }

  @JsonProperty
  public List<URI> getPrefixes()
  {
    return prefixes;
  }

  @Nullable
  @JsonProperty
  public List<CloudObjectLocation> getObjects()
  {
    return objects;
  }

  /**
   * Create the correct {@link InputEntity} for this input source given a split on a {@link CloudObjectLocation}. This
   * is called internally by {@link #formattableReader} and operates on the output of {@link #createSplits}.
   */
  protected abstract InputEntity createEntity(CloudObjectLocation location);

  /**
   * Create a stream of {@link CloudObjectLocation} splits by listing objects that appear under {@link #prefixes} using
   * this input sources backend API. This is called internally by {@link #createSplits} and {@link #estimateNumSplits},
   * only if {@link #prefixes} is set, otherwise the splits are created directly from {@link #uris} or {@link #objects}.
   * Calling if {@link #prefixes} is not set is likely to either lead to an empty iterator or null pointer exception.
   */
  protected abstract Stream<InputSplit<List<CloudObjectLocation>>> getPrefixesSplitStream(SplitHintSpec splitHintSpec);

  @Override
  public Stream<InputSplit<List<CloudObjectLocation>>> createSplits(
      InputFormat inputFormat,
      @Nullable SplitHintSpec splitHintSpec
  )
  {
    if (!CollectionUtils.isNullOrEmpty(objects)) {
      return objects.stream().map(object -> new InputSplit<>(Collections.singletonList(object)));
    }
    if (!CollectionUtils.isNullOrEmpty(uris)) {
      return uris.stream()
                 .map(CloudObjectLocation::new)
                 .map(object -> new InputSplit<>(Collections.singletonList(object)));
    }

    return getPrefixesSplitStream(getSplitHintSpecOrDefault(splitHintSpec));
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    if (!CollectionUtils.isNullOrEmpty(objects)) {
      return objects.size();
    }

    if (!CollectionUtils.isNullOrEmpty(uris)) {
      return uris.size();
    }

    return Ints.checkedCast(getPrefixesSplitStream(getSplitHintSpecOrDefault(splitHintSpec)).count());
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
        createSplits(inputFormat, null).flatMap(split -> split.get().stream()).map(this::createEntity).iterator(),
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
    CloudObjectInputSource that = (CloudObjectInputSource) o;
    return Objects.equals(uris, that.uris) &&
           Objects.equals(prefixes, that.prefixes) &&
           Objects.equals(objects, that.objects);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(uris, prefixes, objects);
  }

  private void throwIfIllegalArgs(boolean clause) throws IllegalArgumentException
  {
    if (clause) {
      throw new IllegalArgumentException("exactly one of either uris or prefixes or objects must be specified");
    }
  }
}
