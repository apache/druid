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
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.storage.google.GoogleStorage;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class GoogleCloudStoreInputSource extends AbstractInputSource implements SplittableInputSource<URI>
{
  private final GoogleStorage storage;
  private final List<URI> uris;

  @JsonCreator
  public GoogleCloudStoreInputSource(
      @JacksonInject("googleStorage") GoogleStorage storage,
      @JsonProperty("uris") List<URI> uris
  )
  {
    this.storage = storage;
    this.uris = uris;
  }

  @JsonProperty("uris")
  public List<URI> getUris()
  {
    return uris;
  }


  @Override
  public Stream<InputSplit<URI>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return uris.stream().map(InputSplit::new);
  }

  @Override
  public int getNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return uris.size();
  }

  @Override
  public SplittableInputSource<URI> withSplit(InputSplit<URI> split)
  {
    return new GoogleCloudStoreInputSource(storage, ImmutableList.of(split.get()));
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
        createSplits(inputFormat, null).map(split -> new GoogleCloudStoreEntity(storage, split.get())),
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
    GoogleCloudStoreInputSource that = (GoogleCloudStoreInputSource) o;
    return Objects.equals(uris, that.uris);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(uris);
  }
}
