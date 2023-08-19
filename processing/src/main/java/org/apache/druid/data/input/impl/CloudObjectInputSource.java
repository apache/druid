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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.FilePerSplitHintSpec;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFileAttribute;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.utils.CollectionUtils;
import org.apache.druid.utils.Streams;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class CloudObjectInputSource extends AbstractInputSource
    implements SplittableInputSource<List<CloudObjectLocation>>
{
  private final String scheme;
  private final List<URI> uris;
  private final List<URI> prefixes;
  private final List<CloudObjectLocation> objects;
  private final String objectGlob;

  public CloudObjectInputSource(
      String scheme,
      @Nullable List<URI> uris,
      @Nullable List<URI> prefixes,
      @Nullable List<CloudObjectLocation> objects,
      @Nullable String objectGlob
  )
  {
    this.scheme = scheme;
    this.uris = uris;
    this.prefixes = prefixes;
    this.objects = objects;
    this.objectGlob = objectGlob;

    illegalArgsChecker();
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<URI> getUris()
  {
    return uris;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<URI> getPrefixes()
  {
    return prefixes;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<CloudObjectLocation> getObjects()
  {
    return objects;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getObjectGlob()
  {
    return objectGlob;
  }

  /**
   * Create the correct {@link InputEntity} for this input source given a split on a {@link CloudObjectLocation}. This
   * is called internally by {@link #formattableReader} and operates on the output of {@link #createSplits}.
   */
  protected abstract InputEntity createEntity(CloudObjectLocation location);

  /**
   * Returns {@link CloudObjectSplitWidget}, which is used to implement
   * {@link #createSplits(InputFormat, SplitHintSpec)}.
   */
  protected abstract CloudObjectSplitWidget getSplitWidget();

  @Override
  public Stream<InputSplit<List<CloudObjectLocation>>> createSplits(
      InputFormat inputFormat,
      @Nullable SplitHintSpec splitHintSpec
  )
  {
    if (!CollectionUtils.isNullOrEmpty(objects)) {
      return getSplitsForObjects(
          inputFormat,
          getSplitWidget(),
          getSplitHintSpecOrDefault(splitHintSpec),
          objects,
          objectGlob
      );
    } else if (!CollectionUtils.isNullOrEmpty(uris)) {
      return getSplitsForObjects(
          inputFormat,
          getSplitWidget(),
          getSplitHintSpecOrDefault(splitHintSpec),
          Lists.transform(uris, CloudObjectLocation::new),
          objectGlob
      );
    } else {
      return getSplitsForPrefixes(
          inputFormat,
          getSplitWidget(),
          getSplitHintSpecOrDefault(splitHintSpec),
          prefixes,
          objectGlob
      );
    }
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    // We can't effectively estimate the number of splits without actually computing them.
    return Ints.checkedCast(createSplits(inputFormat, splitHintSpec).count());
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
        getInputEntities(inputFormat),
        temporaryDirectory
    );
  }

  /**
   * Return an iterator of {@link InputEntity} corresponding to the objects represented by this input source, as read
   * by the provided {@link InputFormat}.
   */
  Iterator<InputEntity> getInputEntities(final InputFormat inputFormat)
  {
    // Use createSplits with FilePerSplitHintSpec.INSTANCE as a way of getting the list of objects to read
    // out of either "prefixes", "objects", or "uris". The specific splits don't matter because we are going
    // to flatten them anyway.
    return createSplits(inputFormat, FilePerSplitHintSpec.INSTANCE)
        .flatMap(split -> split.get().stream())
        .map(this::createEntity)
        .iterator();
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
    return Objects.equals(scheme, that.scheme) &&
           Objects.equals(uris, that.uris) &&
           Objects.equals(prefixes, that.prefixes) &&
           Objects.equals(objects, that.objects) &&
           Objects.equals(objectGlob, that.objectGlob);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(scheme, uris, prefixes, objects, objectGlob);
  }

  private void illegalArgsChecker() throws IllegalArgumentException
  {
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

  private void throwIfIllegalArgs(boolean clause) throws IllegalArgumentException
  {
    if (clause) {
      throw new IllegalArgumentException("Exactly one of uris, prefixes or objects must be specified");
    }
  }

  /**
   * Stream of {@link InputSplit} for situations where this object is based on {@link #getPrefixes()}.
   *
   * If {@link CloudObjectSplitWidget#getDescriptorIteratorForPrefixes} returns objects with known sizes (as most
   * implementations do), this method filters out empty objects.
   */
  private static Stream<InputSplit<List<CloudObjectLocation>>> getSplitsForPrefixes(
      final InputFormat inputFormat,
      final CloudObjectSplitWidget splitWidget,
      final SplitHintSpec splitHintSpec,
      final List<URI> prefixes,
      @Nullable final String objectGlob
  )
  {
    Iterator<CloudObjectSplitWidget.LocationWithSize> iterator =
        splitWidget.getDescriptorIteratorForPrefixes(prefixes);

    if (StringUtils.isNotBlank(objectGlob)) {
      final PathMatcher m = FileSystems.getDefault().getPathMatcher("glob:" + objectGlob);
      iterator = Iterators.filter(
          iterator,
          location -> m.matches(Paths.get(location.getLocation().getPath()))
      );
    }

    // Only consider nonempty objects. Note: size may be unknown; if so we allow it through, to avoid
    // calling getObjectSize and triggering a network call.
    return toSplitStream(
        inputFormat,
        splitWidget,
        splitHintSpec,
        Iterators.filter(iterator, object -> object.getSize() != 0) // Allow UNKNOWN_SIZE through
    );
  }

  /**
   * Stream of {@link InputSplit} for situations where this object is based
   * on {@link #getUris()} or {@link #getObjects()}.
   *
   * This method does not make network calls. In particular, unlike {@link #getSplitsForPrefixes}, this method does
   * not filter out empty objects, because doing so would require calling {@link CloudObjectSplitWidget#getObjectSize}.
   * The hope, and assumption, here is that users won't specify empty objects explicitly. (They're more likely to
   * come in through prefixes.)
   */
  private static Stream<InputSplit<List<CloudObjectLocation>>> getSplitsForObjects(
      final InputFormat inputFormat,
      final CloudObjectSplitWidget splitWidget,
      final SplitHintSpec splitHintSpec,
      final List<CloudObjectLocation> objectLocations,
      @Nullable final String objectGlob
  )
  {
    Iterator<CloudObjectLocation> iterator = objectLocations.iterator();

    if (StringUtils.isNotBlank(objectGlob)) {
      final PathMatcher m = FileSystems.getDefault().getPathMatcher("glob:" + objectGlob);
      iterator = Iterators.filter(
          iterator,
          location -> m.matches(Paths.get(location.getPath()))
      );
    }

    return toSplitStream(
        inputFormat,
        splitWidget,
        splitHintSpec,
        Iterators.transform(
            iterator,
            location -> new CloudObjectSplitWidget.LocationWithSize(location, CloudObjectSplitWidget.UNKNOWN_SIZE)
        )
    );
  }

  private static Stream<InputSplit<List<CloudObjectLocation>>> toSplitStream(
      final InputFormat inputFormat,
      final CloudObjectSplitWidget splitWidget,
      final SplitHintSpec splitHintSpec,
      final Iterator<CloudObjectSplitWidget.LocationWithSize> objectIterator
  )
  {
    return Streams.sequentialStreamFrom(
        splitHintSpec.split(
            objectIterator,
            o -> {
              try {
                if (o.getSize() == CloudObjectSplitWidget.UNKNOWN_SIZE) {
                  long size = splitWidget.getObjectSize(o.getLocation());
                  return new InputFileAttribute(
                      size,
                      inputFormat != null ? inputFormat.getWeightedSize(o.getLocation().getPath(), size) : size
                  );
                } else {
                  return new InputFileAttribute(
                      o.getSize(),
                      inputFormat != null
                      ? inputFormat.getWeightedSize(o.getLocation().getPath(), o.getSize())
                      : o.getSize()
                  );
                }
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
        )
    ).map(
        locations -> new InputSplit<>(
            locations.stream()
                     .map(CloudObjectSplitWidget.LocationWithSize::getLocation)
                     .collect(Collectors.toList()))
    );
  }
}
