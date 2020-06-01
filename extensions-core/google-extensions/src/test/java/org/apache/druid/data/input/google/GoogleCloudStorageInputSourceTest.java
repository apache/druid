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
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.utils.CompressionUtils;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GoogleCloudStorageInputSourceTest extends InitializedNullHandlingTest
{
  private static final int MAX_LISTING_LENGTH = 10;
  private static final GoogleStorage STORAGE = EasyMock.createMock(GoogleStorage.class);
  private static final GoogleInputDataConfig INPUT_DATA_CONFIG = EasyMock.createMock(GoogleInputDataConfig.class);

  private static final List<URI> EXPECTED_URIS = Arrays.asList(
      URI.create("gs://foo/bar/file.csv"),
      URI.create("gs://bar/foo/file2.csv")
  );

  private static final List<URI> EXPECTED_COMPRESSED_URIS = Arrays.asList(
      URI.create("gs://foo/bar/file.csv.gz"),
      URI.create("gs://bar/foo/file2.csv.gz")
  );

  private static final List<List<CloudObjectLocation>> EXPECTED_OBJECTS =
      EXPECTED_URIS.stream()
                   .map(uri -> Collections.singletonList(new CloudObjectLocation(uri)))
                   .collect(Collectors.toList());

  private static final List<URI> PREFIXES = Arrays.asList(
      URI.create("gs://foo/bar"),
      URI.create("gs://bar/foo")
  );

  private static final List<CloudObjectLocation> EXPECTED_LOCATION =
      ImmutableList.of(new CloudObjectLocation("foo", "bar/file.csv"));

  private static final DateTime NOW = DateTimes.nowUtc();
  private static final byte[] CONTENT =
      StringUtils.toUtf8(StringUtils.format("%d,hello,world", NOW.getMillis()));

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = createGoogleObjectMapper();
    final GoogleCloudStorageInputSource withUris =
        new GoogleCloudStorageInputSource(STORAGE, INPUT_DATA_CONFIG, EXPECTED_URIS, ImmutableList.of(), null);
    final GoogleCloudStorageInputSource serdeWithUris =
        mapper.readValue(mapper.writeValueAsString(withUris), GoogleCloudStorageInputSource.class);
    Assert.assertEquals(withUris, serdeWithUris);
  }

  @Test
  public void testSerdePrefixes() throws Exception
  {
    final ObjectMapper mapper = createGoogleObjectMapper();
    final GoogleCloudStorageInputSource withPrefixes =
        new GoogleCloudStorageInputSource(STORAGE, INPUT_DATA_CONFIG, ImmutableList.of(), PREFIXES, null);
    final GoogleCloudStorageInputSource serdeWithPrefixes =
        mapper.readValue(mapper.writeValueAsString(withPrefixes), GoogleCloudStorageInputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
  }

  @Test
  public void testSerdeObjects() throws Exception
  {
    final ObjectMapper mapper = createGoogleObjectMapper();
    final GoogleCloudStorageInputSource withObjects =
        new GoogleCloudStorageInputSource(
            STORAGE,
            INPUT_DATA_CONFIG,
            null,
            null,
            ImmutableList.of(new CloudObjectLocation("foo", "bar/file.gz"))
        );
    final GoogleCloudStorageInputSource serdeWithObjects =
        mapper.readValue(mapper.writeValueAsString(withObjects), GoogleCloudStorageInputSource.class);
    Assert.assertEquals(withObjects, serdeWithObjects);
  }

  @Test
  public void testWithUrisSplit()
  {

    GoogleCloudStorageInputSource inputSource =
        new GoogleCloudStorageInputSource(STORAGE, INPUT_DATA_CONFIG, EXPECTED_URIS, ImmutableList.of(), null);

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null),
        null
    );
    Assert.assertEquals(EXPECTED_OBJECTS, splits.map(InputSplit::get).collect(Collectors.toList()));
  }

  @Test
  public void testWithPrefixesSplit() throws IOException
  {
    EasyMock.reset(STORAGE);
    EasyMock.reset(INPUT_DATA_CONFIG);
    addExpectedPrefixObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)));
    addExpectedPrefixObjects(PREFIXES.get(1), ImmutableList.of(EXPECTED_URIS.get(1)));
    EasyMock.expect(INPUT_DATA_CONFIG.getMaxListingLength()).andReturn(MAX_LISTING_LENGTH);
    EasyMock.replay(STORAGE);
    EasyMock.replay(INPUT_DATA_CONFIG);

    GoogleCloudStorageInputSource inputSource =
        new GoogleCloudStorageInputSource(STORAGE, INPUT_DATA_CONFIG, null, PREFIXES, null);

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null),
        new MaxSizeSplitHintSpec(1L) // set maxSplitSize to 1 so that each inputSplit has only one object
    );

    Assert.assertEquals(EXPECTED_OBJECTS, splits.map(InputSplit::get).collect(Collectors.toList()));
  }

  @Test
  public void testCreateSplitsWithSplitHintSpecRespectingHint() throws IOException
  {
    EasyMock.reset(STORAGE);
    EasyMock.reset(INPUT_DATA_CONFIG);
    addExpectedPrefixObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)));
    addExpectedPrefixObjects(PREFIXES.get(1), ImmutableList.of(EXPECTED_URIS.get(1)));
    EasyMock.expect(INPUT_DATA_CONFIG.getMaxListingLength()).andReturn(MAX_LISTING_LENGTH);
    EasyMock.replay(STORAGE);
    EasyMock.replay(INPUT_DATA_CONFIG);

    GoogleCloudStorageInputSource inputSource =
        new GoogleCloudStorageInputSource(STORAGE, INPUT_DATA_CONFIG, null, PREFIXES, null);

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null),
        new MaxSizeSplitHintSpec(CONTENT.length * 3L)
    );

    Assert.assertEquals(
        ImmutableList.of(EXPECTED_URIS.stream().map(CloudObjectLocation::new).collect(Collectors.toList())),
        splits.map(InputSplit::get).collect(Collectors.toList())
    );
  }

  @Test
  public void testReader() throws IOException
  {
    EasyMock.reset(STORAGE);
    EasyMock.reset(INPUT_DATA_CONFIG);
    addExpectedPrefixObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)));
    addExpectedGetObjectMock(EXPECTED_URIS.get(0));
    addExpectedPrefixObjects(PREFIXES.get(1), ImmutableList.of(EXPECTED_URIS.get(1)));
    addExpectedGetObjectMock(EXPECTED_URIS.get(1));
    EasyMock.expect(INPUT_DATA_CONFIG.getMaxListingLength()).andReturn(MAX_LISTING_LENGTH);
    EasyMock.replay(STORAGE);
    EasyMock.replay(INPUT_DATA_CONFIG);

    GoogleCloudStorageInputSource inputSource = new GoogleCloudStorageInputSource(
        STORAGE,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null
    );

    InputRowSchema someSchema = new InputRowSchema(
        new TimestampSpec("time", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2"))),
        ImmutableList.of("count")
    );

    InputSourceReader reader = inputSource.reader(
        someSchema,
        new CsvInputFormat(ImmutableList.of("time", "dim1", "dim2"), "|", false, null, 0),
        null
    );

    CloseableIterator<InputRow> iterator = reader.read();

    while (iterator.hasNext()) {
      InputRow nextRow = iterator.next();
      Assert.assertEquals(NOW, nextRow.getTimestamp());
      Assert.assertEquals("hello", nextRow.getDimension("dim1").get(0));
      Assert.assertEquals("world", nextRow.getDimension("dim2").get(0));
    }
  }

  @Test
  public void testCompressedReader() throws IOException
  {
    EasyMock.reset(STORAGE);
    EasyMock.reset(INPUT_DATA_CONFIG);
    addExpectedPrefixObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_COMPRESSED_URIS.get(0)));
    addExpectedGetCompressedObjectMock(EXPECTED_COMPRESSED_URIS.get(0));
    addExpectedPrefixObjects(PREFIXES.get(1), ImmutableList.of(EXPECTED_COMPRESSED_URIS.get(1)));
    addExpectedGetCompressedObjectMock(EXPECTED_COMPRESSED_URIS.get(1));
    EasyMock.expect(INPUT_DATA_CONFIG.getMaxListingLength()).andReturn(MAX_LISTING_LENGTH);
    EasyMock.replay(STORAGE);
    EasyMock.replay(INPUT_DATA_CONFIG);

    GoogleCloudStorageInputSource inputSource = new GoogleCloudStorageInputSource(
        STORAGE,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null
    );

    InputRowSchema someSchema = new InputRowSchema(
        new TimestampSpec("time", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2"))),
        ImmutableList.of("count")
    );

    InputSourceReader reader = inputSource.reader(
        someSchema,
        new CsvInputFormat(ImmutableList.of("time", "dim1", "dim2"), "|", false, null, 0),
        null
    );

    CloseableIterator<InputRow> iterator = reader.read();

    while (iterator.hasNext()) {
      InputRow nextRow = iterator.next();
      Assert.assertEquals(NOW, nextRow.getTimestamp());
      Assert.assertEquals("hello", nextRow.getDimension("dim1").get(0));
      Assert.assertEquals("world", nextRow.getDimension("dim2").get(0));
    }
  }

  private static void addExpectedPrefixObjects(URI prefix, List<URI> uris) throws IOException
  {
    final String bucket = prefix.getAuthority();

    Storage.Objects.List listRequest = EasyMock.createMock(Storage.Objects.List.class);
    EasyMock.expect(STORAGE.list(EasyMock.eq(bucket))).andReturn(listRequest).once();
    EasyMock.expect(listRequest.setPageToken(EasyMock.anyString())).andReturn(listRequest).once();
    EasyMock.expect(listRequest.setMaxResults((long) MAX_LISTING_LENGTH)).andReturn(listRequest).once();
    EasyMock.expect(listRequest.setPrefix(EasyMock.eq(StringUtils.maybeRemoveLeadingSlash(prefix.getPath()))))
            .andReturn(listRequest)
            .once();

    List<StorageObject> mockObjects = new ArrayList<>();
    for (URI uri : uris) {
      StorageObject s = new StorageObject();
      s.setBucket(bucket);
      s.setName(uri.getPath());
      s.setSize(BigInteger.valueOf(CONTENT.length));
      mockObjects.add(s);
    }
    Objects response = new Objects();
    response.setItems(mockObjects);
    EasyMock.expect(listRequest.execute()).andReturn(response).once();
    EasyMock.expect(response.getItems()).andReturn(mockObjects).once();

    EasyMock.replay(listRequest);
  }

  private static void addExpectedGetObjectMock(URI uri) throws IOException
  {
    CloudObjectLocation location = new CloudObjectLocation(uri);

    EasyMock.expect(
        STORAGE.get(EasyMock.eq(location.getBucket()), EasyMock.eq(location.getPath()), EasyMock.eq(0L))
    ).andReturn(new ByteArrayInputStream(CONTENT)).once();
  }

  private static void addExpectedGetCompressedObjectMock(URI uri) throws IOException
  {
    CloudObjectLocation location = new CloudObjectLocation(uri);

    ByteArrayOutputStream gzipped = new ByteArrayOutputStream();
    CompressionUtils.gzip(new ByteArrayInputStream(CONTENT), gzipped);

    EasyMock.expect(
        STORAGE.get(EasyMock.eq(location.getBucket()), EasyMock.eq(location.getPath()), EasyMock.eq(0L))
    ).andReturn(new ByteArrayInputStream(gzipped.toByteArray())).once();
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
    public GoogleStorage getGoogleStorage()
    {
      return STORAGE;
    }
  }
}
