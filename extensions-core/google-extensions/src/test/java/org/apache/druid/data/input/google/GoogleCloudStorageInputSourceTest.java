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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.storage.google.GoogleStorage;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GoogleCloudStorageInputSourceTest
{
  private static final GoogleStorage STORAGE = new GoogleStorage(null);

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = createGoogleObjectMapper();

    final List<URI> uris = Arrays.asList(
        new URI("gs://foo/bar/file.gz"),
        new URI("gs://bar/foo/file2.gz")
    );

    final List<URI> prefixes = Arrays.asList(
        new URI("gs://foo/bar"),
        new URI("gs://bar/foo")
    );

    final GoogleCloudStorageInputSource withUris = new GoogleCloudStorageInputSource(STORAGE, uris);
    final GoogleCloudStorageInputSource serdeWithUris =
        mapper.readValue(mapper.writeValueAsString(withUris), GoogleCloudStorageInputSource.class);
    Assert.assertEquals(withUris, serdeWithUris);
  }

  @Test
  public void testWithUrisSplit()
  {
    final List<URI> uris = Arrays.asList(
        URI.create("gs://foo/bar/file.gz"),
        URI.create("gs://bar/foo/file2.gz")
    );

    GoogleCloudStorageInputSource inputSource = new GoogleCloudStorageInputSource(STORAGE, uris);

    Stream<InputSplit<URI>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null),
        null
    );
    Assert.assertEquals(uris, splits.map(InputSplit::get).collect(Collectors.toList()));
  }

  public static ObjectMapper createGoogleObjectMapper()
  {
    final DruidModule baseModule = new TestGoogleModule();
    final ObjectMapper baseMapper = new DefaultObjectMapper();
    baseModule.getJacksonModules().forEach(baseMapper::registerModule);

    final Injector injector = Guice.createInjector(
        new ObjectMapperModule(),
        baseModule
    );
    return injector.getInstance(ObjectMapper.class);
  }

  private static class TestGoogleModule implements DruidModule
  {
    @Override
    public List<? extends Module> getJacksonModules()
    {
      return ImmutableList.of(new SimpleModule());
    }

    @Override
    public void configure(Binder binder)
    {

    }

    @Provides
    public GoogleStorage getRestS3Service()
    {
      return STORAGE;
    }
  }
}