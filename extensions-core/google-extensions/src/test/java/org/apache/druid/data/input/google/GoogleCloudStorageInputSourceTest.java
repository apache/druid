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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputStatsImpl;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleStorageObjectMetadata;
import org.apache.druid.storage.google.GoogleStorageObjectPage;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.utils.CompressionUtils;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
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

  private static final List<URI> URIS_BEFORE_GLOB = Arrays.asList(
      URI.create("gs://foo/bar/file.csv"),
      URI.create("gs://bar/foo/file2.csv"),
      URI.create("gs://bar/foo/file3.txt")
  );

  private static final List<List<CloudObjectLocation>> EXPECTED_OBJECTS =
      EXPECTED_URIS.stream()
                   .map(uri -> Collections.singletonList(new CloudObjectLocation(uri)))
                   .collect(Collectors.toList());

  private static final List<URI> PREFIXES = Arrays.asList(
      URI.create("gs://foo/bar"),
      URI.create("gs://bar/foo")
  );

  private static final DateTime NOW = DateTimes.nowUtc();
  private static final byte[] CONTENT =
      StringUtils.toUtf8(StringUtils.format("%d,hello,world", NOW.getMillis()));

  private static final String BUCKET = "TEST_BUCKET";
  private static final String OBJECT_NAME = "TEST_NAME";
  private static final Long UPDATE_TIME = 111L;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = createGoogleObjectMapper();
    final GoogleCloudStorageInputSource withUris =
        new GoogleCloudStorageInputSource(
            STORAGE,
            INPUT_DATA_CONFIG,
            EXPECTED_URIS,
            ImmutableList.of(),
            null,
            null,
            null
        );
    final GoogleCloudStorageInputSource serdeWithUris =
        mapper.readValue(mapper.writeValueAsString(withUris), GoogleCloudStorageInputSource.class);
    Assert.assertEquals(withUris, serdeWithUris);
  }

  @Test
  public void testSerdePrefixes() throws Exception
  {
    final ObjectMapper mapper = createGoogleObjectMapper();
    final GoogleCloudStorageInputSource withPrefixes =
        new GoogleCloudStorageInputSource(STORAGE, INPUT_DATA_CONFIG, ImmutableList.of(), PREFIXES, null, null, null);
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
            ImmutableList.of(new CloudObjectLocation("foo", "bar/file.gz")),
            null,
            null
        );
    final GoogleCloudStorageInputSource serdeWithObjects =
        mapper.readValue(mapper.writeValueAsString(withObjects), GoogleCloudStorageInputSource.class);
    Assert.assertEquals(withObjects, serdeWithObjects);
    Assert.assertEquals(Collections.emptySet(), serdeWithObjects.getConfiguredSystemFields());
  }

  @Test
  public void testSerdeObjectsAndSystemFields() throws Exception
  {
    final ObjectMapper mapper = createGoogleObjectMapper();
    final GoogleCloudStorageInputSource withObjects =
        (GoogleCloudStorageInputSource) new GoogleCloudStorageInputSource(
            STORAGE,
            INPUT_DATA_CONFIG,
            null,
            null,
            ImmutableList.of(new CloudObjectLocation("foo", "bar/file.gz")),
            null,
            new SystemFields(EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH))
        );
    final GoogleCloudStorageInputSource serdeWithObjects =
        mapper.readValue(mapper.writeValueAsString(withObjects), GoogleCloudStorageInputSource.class);
    Assert.assertEquals(withObjects, serdeWithObjects);
    Assert.assertEquals(
        EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH),
        serdeWithObjects.getConfiguredSystemFields()
    );
  }

  @Test
  public void testGetTypes()
  {
    final GoogleCloudStorageInputSource inputSource =
        new GoogleCloudStorageInputSource(
            STORAGE,
            INPUT_DATA_CONFIG,
            EXPECTED_URIS,
            ImmutableList.of(),
            null,
            null,
            null
        );
    Assert.assertEquals(Collections.singleton(GoogleCloudStorageInputSource.TYPE_KEY), inputSource.getTypes());
  }

  @Test
  public void testWithUrisSplit() throws IOException
  {
    EasyMock.reset(STORAGE);

    GoogleStorageObjectMetadata objectMetadata = new GoogleStorageObjectMetadata(
        BUCKET,
        OBJECT_NAME,
        (long) CONTENT.length,
        UPDATE_TIME
    );

    EasyMock.expect(
        STORAGE.getMetadata(
            EXPECTED_URIS.get(0).getAuthority(),
            StringUtils.maybeRemoveLeadingSlash(EXPECTED_URIS.get(0).getPath())
        )
    ).andReturn(objectMetadata);

    EasyMock.expect(
        STORAGE.getMetadata(
            EXPECTED_URIS.get(1).getAuthority(),
            StringUtils.maybeRemoveLeadingSlash(EXPECTED_URIS.get(1).getPath())
        )
    ).andReturn(objectMetadata);

    EasyMock.replay(STORAGE);
    GoogleCloudStorageInputSource inputSource =
        new GoogleCloudStorageInputSource(
            STORAGE,
            INPUT_DATA_CONFIG,
            EXPECTED_URIS,
            ImmutableList.of(),
            null,
            null,
            null
        );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        null,
        new MaxSizeSplitHintSpec(10, null)
    );
    Assert.assertEquals(EXPECTED_OBJECTS, splits.map(InputSplit::get).collect(Collectors.toList()));
    EasyMock.verify(STORAGE);
  }

  @Test
  public void testWithUrisGlob() throws IOException
  {
    GoogleStorageObjectMetadata objectMetadata = new GoogleStorageObjectMetadata(
        BUCKET,
        OBJECT_NAME,
        (long) CONTENT.length,
        UPDATE_TIME
    );

    EasyMock.reset(STORAGE);
    EasyMock.expect(
        STORAGE.getMetadata(
            EXPECTED_URIS.get(0).getAuthority(),
            StringUtils.maybeRemoveLeadingSlash(EXPECTED_URIS.get(0).getPath())
        )
    ).andReturn(objectMetadata);
    EasyMock.expect(
        STORAGE.getMetadata(
            EXPECTED_URIS.get(1).getAuthority(),
            StringUtils.maybeRemoveLeadingSlash(EXPECTED_URIS.get(1).getPath())
        )
    ).andReturn(objectMetadata);
    EasyMock.replay(STORAGE);
    GoogleCloudStorageInputSource inputSource = new GoogleCloudStorageInputSource(
        STORAGE,
        INPUT_DATA_CONFIG,
        URIS_BEFORE_GLOB,
        null,
        null,
        "**.csv",
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        null,
        new MaxSizeSplitHintSpec(10, null)
    );
    Assert.assertEquals(EXPECTED_OBJECTS, splits.map(InputSplit::get).collect(Collectors.toList()));
    EasyMock.verify(STORAGE);
  }

  @Test
  public void testIllegalObjectsAndPrefixes()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new GoogleCloudStorageInputSource(
        STORAGE,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        EXPECTED_OBJECTS.get(0),
        "**.csv",
        null
    );
  }

  @Test
  public void testIllegalUrisAndPrefixes()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new GoogleCloudStorageInputSource(
        STORAGE,
        INPUT_DATA_CONFIG,
        URIS_BEFORE_GLOB,
        PREFIXES,
        null,
        "**.csv",
        null
    );
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
        new GoogleCloudStorageInputSource(STORAGE, INPUT_DATA_CONFIG, null, PREFIXES, null, null, null);

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
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
        new GoogleCloudStorageInputSource(STORAGE, INPUT_DATA_CONFIG, null, PREFIXES, null, null, null);

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(new HumanReadableBytes(CONTENT.length * 3L), null)
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
        null,
        null,
        null
    );

    InputRowSchema someSchema = new InputRowSchema(
        new TimestampSpec("time", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2"))),
        ColumnsFilter.all()
    );

    InputSourceReader reader = inputSource.reader(
        someSchema,
        new CsvInputFormat(ImmutableList.of("time", "dim1", "dim2"), "|", false, null, 0),
        null
    );

    final InputStats inputStats = new InputStatsImpl();
    CloseableIterator<InputRow> iterator = reader.read(inputStats);

    while (iterator.hasNext()) {
      InputRow nextRow = iterator.next();
      Assert.assertEquals(NOW, nextRow.getTimestamp());
      Assert.assertEquals("hello", nextRow.getDimension("dim1").get(0));
      Assert.assertEquals("world", nextRow.getDimension("dim2").get(0));
    }
    Assert.assertEquals(2 * CONTENT.length, inputStats.getProcessedBytes());
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
        null,
        null,
        null
    );

    InputRowSchema someSchema = new InputRowSchema(
        new TimestampSpec("time", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2"))),
        ColumnsFilter.all()
    );

    InputSourceReader reader = inputSource.reader(
        someSchema,
        new CsvInputFormat(ImmutableList.of("time", "dim1", "dim2"), "|", false, null, 0),
        null
    );

    final InputStats inputStats = new InputStatsImpl();
    CloseableIterator<InputRow> iterator = reader.read(inputStats);

    while (iterator.hasNext()) {
      InputRow nextRow = iterator.next();
      Assert.assertEquals(NOW, nextRow.getTimestamp());
      Assert.assertEquals("hello", nextRow.getDimension("dim1").get(0));
      Assert.assertEquals("world", nextRow.getDimension("dim2").get(0));
    }
    Assert.assertEquals(2 * CONTENT.length, inputStats.getProcessedBytes());
  }

  @Test
  public void testSystemFields()
  {
    GoogleCloudStorageInputSource inputSource = new GoogleCloudStorageInputSource(
        STORAGE,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null,
        null,
        new SystemFields(EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH))
    );

    Assert.assertEquals(
        EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH),
        inputSource.getConfiguredSystemFields()
    );

    final GoogleCloudStorageEntity entity = new GoogleCloudStorageEntity(null, new CloudObjectLocation("foo", "bar"));

    Assert.assertEquals("gs://foo/bar", inputSource.getSystemFieldValue(entity, SystemField.URI));
    Assert.assertEquals("foo", inputSource.getSystemFieldValue(entity, SystemField.BUCKET));
    Assert.assertEquals("bar", inputSource.getSystemFieldValue(entity, SystemField.PATH));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(GoogleCloudStorageInputSource.class)
                  .withIgnoredFields("storage", "inputDataConfig")
                  .usingGetClass()
                  .verify();
  }

  private static void addExpectedPrefixObjects(URI prefix, List<URI> uris) throws IOException
  {
    final String bucket = prefix.getAuthority();

    GoogleStorageObjectPage response = EasyMock.createMock(GoogleStorageObjectPage.class);

    List<GoogleStorageObjectMetadata> mockObjects = new ArrayList<>();
    for (URI uri : uris) {
      GoogleStorageObjectMetadata s = new GoogleStorageObjectMetadata(
          bucket,
          uri.getPath(),
          (long) CONTENT.length,
          UPDATE_TIME
      );
      mockObjects.add(s);
    }

    EasyMock.expect(STORAGE.list(
        EasyMock.eq(bucket),
        EasyMock.eq(StringUtils.maybeRemoveLeadingSlash(prefix.getPath())),
        EasyMock.eq((long) MAX_LISTING_LENGTH),
        EasyMock.eq(null)
    )).andReturn(response).once();

    EasyMock.expect(response.getObjectList()).andReturn(mockObjects).once();
    EasyMock.expect(response.getNextPageToken()).andReturn(null).once();

    EasyMock.replay(response);
  }

  private static void addExpectedGetObjectMock(URI uri) throws IOException
  {
    CloudObjectLocation location = new CloudObjectLocation(uri);

    EasyMock.expect(
        STORAGE.getInputStream(EasyMock.eq(location.getBucket()), EasyMock.eq(location.getPath()), EasyMock.eq(0L))
    ).andReturn(new ByteArrayInputStream(CONTENT)).once();
  }

  private static void addExpectedGetCompressedObjectMock(URI uri) throws IOException
  {
    CloudObjectLocation location = new CloudObjectLocation(uri);

    ByteArrayOutputStream gzipped = new ByteArrayOutputStream();
    CompressionUtils.gzip(new ByteArrayInputStream(CONTENT), gzipped);

    EasyMock.expect(
        STORAGE.getInputStream(EasyMock.eq(location.getBucket()), EasyMock.eq(location.getPath()), EasyMock.eq(0L))
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
