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

package org.apache.druid.data.input.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.utils.CompressionUtils;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class S3InputSourceTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper MAPPER = createS3ObjectMapper();
  private static final AmazonS3Client S3_CLIENT = EasyMock.createNiceMock(AmazonS3Client.class);
  private static final ServerSideEncryptingAmazonS3 SERVICE = new ServerSideEncryptingAmazonS3(
      S3_CLIENT,
      new NoopServerSideEncryption()
  );

  private static final List<URI> EXPECTED_URIS = Arrays.asList(
      URI.create("s3://foo/bar/file.csv"),
      URI.create("s3://bar/foo/file2.csv")
  );

  private static final List<URI> EXPECTED_COMPRESSED_URIS = Arrays.asList(
      URI.create("s3://foo/bar/file.csv.gz"),
      URI.create("s3://bar/foo/file2.csv.gz")
  );

  private static final List<CloudObjectLocation> EXPECTED_COORDS =
      EXPECTED_URIS.stream().map(CloudObjectLocation::new).collect(Collectors.toList());

  private static final List<URI> PREFIXES = Arrays.asList(
      URI.create("s3://foo/bar"),
      URI.create("s3://bar/foo")
  );

  private static final List<CloudObjectLocation> EXPECTED_LOCATION =
      ImmutableList.of(new CloudObjectLocation("foo", "bar/file.csv"));

  private static final DateTime NOW = DateTimes.nowUtc();
  private static final byte[] CONTENT =
      StringUtils.toUtf8(StringUtils.format("%d,hello,world", NOW.getMillis()));

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerdeWithUris() throws Exception
  {
    final S3InputSource withUris = new S3InputSource(SERVICE, EXPECTED_URIS, null, null);
    final S3InputSource serdeWithUris = MAPPER.readValue(MAPPER.writeValueAsString(withUris), S3InputSource.class);
    Assert.assertEquals(withUris, serdeWithUris);
  }

  @Test
  public void testSerdeWithPrefixes() throws Exception
  {
    final S3InputSource withPrefixes = new S3InputSource(SERVICE, null, PREFIXES, null);
    final S3InputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), S3InputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
  }

  @Test
  public void testSerdeWithObjects() throws Exception
  {

    final S3InputSource withPrefixes = new S3InputSource(
        SERVICE,
        null,
        null,
        EXPECTED_LOCATION
    );
    final S3InputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), S3InputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
  }

  @Test
  public void testSerdeWithExtraEmptyLists() throws Exception
  {
    final S3InputSource withPrefixes = new S3InputSource(
        SERVICE,
        ImmutableList.of(),
        ImmutableList.of(),
        EXPECTED_LOCATION
    );
    final S3InputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), S3InputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
  }

  @Test
  public void testSerdeWithInvalidArgs() throws Exception
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new S3InputSource(
        SERVICE,
        EXPECTED_URIS,
        PREFIXES,
        EXPECTED_LOCATION
    );
  }

  @Test
  public void testSerdeWithOtherInvalidArgs()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new S3InputSource(
        SERVICE,
        EXPECTED_URIS,
        PREFIXES,
        ImmutableList.of()
    );
  }

  @Test
  public void testSerdeWithOtherOtherInvalidArgs()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new S3InputSource(
        SERVICE,
        ImmutableList.of(),
        PREFIXES,
        EXPECTED_LOCATION
    );
  }

  @Test
  public void testWithUrisSplit()
  {
    S3InputSource inputSource = new S3InputSource(SERVICE, EXPECTED_URIS, null, null);

    Stream<InputSplit<CloudObjectLocation>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null),
        null
    );

    Assert.assertEquals(EXPECTED_COORDS, splits.map(InputSplit::get).collect(Collectors.toList()));
  }

  @Test
  public void testWithPrefixesSplit()
  {
    EasyMock.reset(S3_CLIENT);
    addExpectedPrefixObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)));
    addExpectedPrefixObjects(PREFIXES.get(1), ImmutableList.of(EXPECTED_URIS.get(1)));
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(SERVICE, null, PREFIXES, null);

    Stream<InputSplit<CloudObjectLocation>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null),
        null
    );

    Assert.assertEquals(EXPECTED_COORDS, splits.map(InputSplit::get).collect(Collectors.toList()));
  }

  @Test
  public void testWithPrefixesWhereOneIsUrisAndNoListPermissionSplit()
  {
    EasyMock.reset(S3_CLIENT);
    addExpectedPrefixObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)));
    addExpectedNonPrefixObjectsWithNoListPermission();
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        null,
        ImmutableList.of(PREFIXES.get(0), EXPECTED_URIS.get(1)),
        null
    );

    Stream<InputSplit<CloudObjectLocation>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null),
        null
    );

    Assert.assertEquals(EXPECTED_COORDS, splits.map(InputSplit::get).collect(Collectors.toList()));
  }

  @Test
  public void testReader() throws IOException
  {
    EasyMock.reset(S3_CLIENT);
    addExpectedPrefixObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)));
    addExpectedNonPrefixObjectsWithNoListPermission();
    addExpectedGetObjectMock(EXPECTED_URIS.get(0));
    addExpectedGetObjectMock(EXPECTED_URIS.get(1));
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        null,
        ImmutableList.of(PREFIXES.get(0), EXPECTED_URIS.get(1)),
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
        temporaryFolder.newFolder()
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
    EasyMock.reset(S3_CLIENT);
    addExpectedPrefixObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_COMPRESSED_URIS.get(0)));
    addExpectedNonPrefixObjectsWithNoListPermission();
    addExpectedGetCompressedObjectMock(EXPECTED_COMPRESSED_URIS.get(0));
    addExpectedGetCompressedObjectMock(EXPECTED_COMPRESSED_URIS.get(1));
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        null,
        ImmutableList.of(PREFIXES.get(0), EXPECTED_COMPRESSED_URIS.get(1)),
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
        temporaryFolder.newFolder()
    );

    CloseableIterator<InputRow> iterator = reader.read();

    while (iterator.hasNext()) {
      InputRow nextRow = iterator.next();
      Assert.assertEquals(NOW, nextRow.getTimestamp());
      Assert.assertEquals("hello", nextRow.getDimension("dim1").get(0));
      Assert.assertEquals("world", nextRow.getDimension("dim2").get(0));
    }
  }

  private static void addExpectedPrefixObjects(URI prefix, List<URI> uris)
  {
    final String s3Bucket = prefix.getAuthority();
    final ListObjectsV2Result result = new ListObjectsV2Result();
    result.setBucketName(s3Bucket);
    result.setKeyCount(1);
    for (URI uri : uris) {
      final String key = S3Utils.extractS3Key(uri);
      final S3ObjectSummary objectSummary = new S3ObjectSummary();
      objectSummary.setBucketName(s3Bucket);
      objectSummary.setKey(key);
      result.getObjectSummaries().add(objectSummary);
    }
    EasyMock.expect(S3_CLIENT.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class))).andReturn(result).once();
  }

  private static void addExpectedNonPrefixObjectsWithNoListPermission()
  {
    AmazonS3Exception boom = new AmazonS3Exception("oh dang, you can't list that bucket friend");
    boom.setStatusCode(403);
    EasyMock.expect(S3_CLIENT.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class))).andThrow(boom).once();

    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(CONTENT.length);
    metadata.setContentEncoding("text/csv");
    metadata.setHeader(Headers.ETAG, "some-totally-real-etag-base64-hash-i-guess");
    EasyMock.expect(S3_CLIENT.getObjectMetadata(EasyMock.anyObject(GetObjectMetadataRequest.class)))
            .andReturn(metadata)
            .once();
  }

  private static void addExpectedGetObjectMock(URI uri)
  {
    final String s3Bucket = uri.getAuthority();
    final String key = S3Utils.extractS3Key(uri);

    S3Object someObject = new S3Object();
    someObject.setBucketName(s3Bucket);
    someObject.setKey(key);
    someObject.setObjectContent(new ByteArrayInputStream(CONTENT));
    EasyMock.expect(S3_CLIENT.getObject(EasyMock.anyObject(GetObjectRequest.class))).andReturn(someObject).once();
  }

  private static void addExpectedGetCompressedObjectMock(URI uri) throws IOException
  {
    final String s3Bucket = uri.getAuthority();
    final String key = S3Utils.extractS3Key(uri);

    S3Object someObject = new S3Object();
    someObject.setBucketName(s3Bucket);
    someObject.setKey(key);
    ByteArrayOutputStream gzipped = new ByteArrayOutputStream();
    CompressionUtils.gzip(new ByteArrayInputStream(CONTENT), gzipped);
    someObject.setObjectContent(new ByteArrayInputStream(gzipped.toByteArray()));
    EasyMock.expect(S3_CLIENT.getObject(EasyMock.anyObject(GetObjectRequest.class))).andReturn(someObject).once();
  }

  public static ObjectMapper createS3ObjectMapper()
  {
    DruidModule baseModule = new TestS3Module();
    final Injector injector = Guice.createInjector(
        new ObjectMapperModule(),
        baseModule
    );
    final ObjectMapper baseMapper = injector.getInstance(ObjectMapper.class);

    baseModule.getJacksonModules().forEach(baseMapper::registerModule);
    return baseMapper;
  }

  public static class TestS3Module implements DruidModule
  {
    @Override
    public List<? extends Module> getJacksonModules()
    {
      // Deserializer is need for AmazonS3Client even though it is injected.
      // See https://github.com/FasterXML/jackson-databind/issues/962.
      return ImmutableList.of(new SimpleModule().addDeserializer(AmazonS3.class, new ItemDeserializer()));
    }

    @Override
    public void configure(Binder binder)
    {

    }

    @Provides
    public ServerSideEncryptingAmazonS3 getAmazonS3Client()
    {
      return SERVICE;
    }
  }

  public static class ItemDeserializer extends StdDeserializer<AmazonS3>
  {
    ItemDeserializer()
    {
      this(null);
    }

    ItemDeserializer(Class<?> vc)
    {
      super(vc);
    }

    @Override
    public AmazonS3 deserialize(JsonParser jp, DeserializationContext ctxt)
    {
      throw new UnsupportedOperationException();
    }
  }
}
