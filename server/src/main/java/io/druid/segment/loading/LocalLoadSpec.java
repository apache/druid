/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.loading;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.api.client.util.Preconditions;
import io.druid.guice.LocalDataStorageDruidModule;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 */
@JsonTypeName(LocalDataStorageDruidModule.SCHEME)
public class LocalLoadSpec implements LoadSpec, URISupplier
{
  private final Path path;
  private final LocalDataSegmentPuller puller;

  @JsonCreator
  public LocalLoadSpec(
      @JacksonInject LocalDataSegmentPuller puller,
      @JsonProperty(value = "path", required = true) final String path
  )
  {
    Preconditions.checkNotNull(path);
    this.path = Paths.get(path);
    Preconditions.checkArgument(Files.exists(Paths.get(path)), "[%s] does not exist", path);
    this.puller = puller;
  }

  @JsonProperty
  public String getPath()
  {
    return path.toString();
  }

  @Override
  public LoadSpecResult loadSegment(final File outDir) throws SegmentLoadingException
  {
    return new LoadSpecResult(puller.getSegmentFiles(path.toFile(), outDir).size());
  }

  @Override
  public URI getURI()
  {
    return path.toUri();
  }
}
