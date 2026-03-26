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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputSourceFactory;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleStorageDruidModule;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GoogleCloudStorageInputSourceFactoryTest
{
  private static final GoogleStorage STORAGE = EasyMock.createMock(GoogleStorage.class);
  private static final GoogleInputDataConfig INPUT_DATA_CONFIG = EasyMock.createMock(GoogleInputDataConfig.class);

  private static ObjectMapper createObjectMapper()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    new GoogleStorageDruidModule().getJacksonModules().forEach(mapper::registerModule);
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(GoogleStorage.class, STORAGE)
            .addValue(GoogleInputDataConfig.class, INPUT_DATA_CONFIG)
    );
    return mapper;
  }

  @Test
  public void testCreateReturnsGoogleCloudStorageInputSource()
  {
    final GoogleCloudStorageInputSourceFactory factory = new GoogleCloudStorageInputSourceFactory(
        STORAGE,
        INPUT_DATA_CONFIG
    );
    final List<String> paths = Arrays.asList(
        "gs://foo/bar/file.csv",
        "gs://bar/foo/file2.csv"
    );

    final SplittableInputSource inputSource = factory.create(paths);
    Assert.assertTrue(inputSource instanceof GoogleCloudStorageInputSource);
  }

  @Test
  public void testCreateWithMultiplePaths()
  {
    final GoogleCloudStorageInputSourceFactory factory = new GoogleCloudStorageInputSourceFactory(
        STORAGE,
        INPUT_DATA_CONFIG
    );
    final List<String> paths = Arrays.asList(
        "gs://foo/bar/file.csv",
        "gs://bar/foo/file2.csv",
        "gs://baz/qux/file3.txt"
    );

    final GoogleCloudStorageInputSource inputSource = (GoogleCloudStorageInputSource) factory.create(paths);
    Assert.assertNotNull(inputSource.getUris());
    Assert.assertEquals(3, inputSource.getUris().size());
    Assert.assertEquals(URI.create("gs://foo/bar/file.csv"), inputSource.getUris().get(0));
    Assert.assertEquals(URI.create("gs://bar/foo/file2.csv"), inputSource.getUris().get(1));
    Assert.assertEquals(URI.create("gs://baz/qux/file3.txt"), inputSource.getUris().get(2));
  }

  @Test
  public void testCreatePreservesGsScheme()
  {
    final GoogleCloudStorageInputSourceFactory factory = new GoogleCloudStorageInputSourceFactory(
        STORAGE,
        INPUT_DATA_CONFIG
    );
    final List<String> paths = Collections.singletonList("gs://bucket/path/to/file.csv");

    final GoogleCloudStorageInputSource inputSource = (GoogleCloudStorageInputSource) factory.create(paths);
    final URI uri = inputSource.getUris().get(0);
    Assert.assertEquals("gs", uri.getScheme());
    Assert.assertEquals("bucket", uri.getHost());
    Assert.assertEquals("/path/to/file.csv", uri.getPath());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateWithEmptyListThrows()
  {
    final GoogleCloudStorageInputSourceFactory factory = new GoogleCloudStorageInputSourceFactory(
        STORAGE,
        INPUT_DATA_CONFIG
    );
    factory.create(Collections.emptyList());
  }

  @Test
  public void testCreateWithEncodedPaths()
  {
    final GoogleCloudStorageInputSourceFactory factory = new GoogleCloudStorageInputSourceFactory(
        STORAGE,
        INPUT_DATA_CONFIG
    );
    final List<String> paths = Collections.singletonList("gs://bucket/path%20with%20spaces/file.csv");

    final GoogleCloudStorageInputSource inputSource = (GoogleCloudStorageInputSource) factory.create(paths);
    final URI uri = inputSource.getUris().get(0);
    Assert.assertEquals("gs://bucket/path%20with%20spaces/file.csv", uri.toString());
  }

  @Test
  public void testJacksonDeserialization() throws Exception
  {
    final ObjectMapper mapper = createObjectMapper();

    final String json = "{\"type\": \"google\"}";
    final InputSourceFactory factory = mapper.readValue(json, InputSourceFactory.class);
    Assert.assertTrue(factory instanceof GoogleCloudStorageInputSourceFactory);
  }

  @Test
  public void testSerializationRoundTrip() throws Exception
  {
    final ObjectMapper mapper = createObjectMapper();

    final GoogleCloudStorageInputSourceFactory original = new GoogleCloudStorageInputSourceFactory(
        STORAGE,
        INPUT_DATA_CONFIG
    );

    final String json = mapper.writeValueAsString(original);
    Assert.assertTrue(json.contains("\"type\":\"google\""));

    final GoogleCloudStorageInputSourceFactory deserialized = mapper.readValue(
        json,
        GoogleCloudStorageInputSourceFactory.class
    );
    Assert.assertNotNull(deserialized);
  }
}
