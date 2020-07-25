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

package org.apache.druid.data.input.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
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
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
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
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.storage.aliyun.OssInputDataConfig;
import org.apache.druid.storage.aliyun.OssUtils;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.utils.CompressionUtils;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OssInputSourceTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper MAPPER = createObjectMapper();
  private static final OSS OSSCLIENT = EasyMock.createMock(OSSClient.class);
  private static final OssInputDataConfig INPUT_DATA_CONFIG;
  private static final int MAX_LISTING_LENGTH = 10;

  private static final List<URI> EXPECTED_URIS = Arrays.asList(
      URI.create("oss://foo/bar/file.csv"),
      URI.create("oss://bar/foo/file2.csv")
  );

  private static final List<URI> EXPECTED_COMPRESSED_URIS = Arrays.asList(
      URI.create("oss://foo/bar/file.csv.gz"),
      URI.create("oss://bar/foo/file2.csv.gz")
  );

  private static final List<List<CloudObjectLocation>> EXPECTED_COORDS =
      EXPECTED_URIS.stream()
                   .map(uri -> Collections.singletonList(new CloudObjectLocation(uri)))
                   .collect(Collectors.toList());

  private static final List<URI> PREFIXES = Arrays.asList(
      URI.create("oss://foo/bar"),
      URI.create("oss://bar/foo")
  );

  private static final OssClientConfig CLOUD_CONFIG_PROPERTIES = new OssClientConfig(
      "test.oss-cn.aliyun.com",
      new DefaultPasswordProvider("myKey"),
      new DefaultPasswordProvider("mySecret"));

  private static final List<CloudObjectLocation> EXPECTED_LOCATION =
      ImmutableList.of(new CloudObjectLocation("foo", "bar/file.csv"));

  private static final DateTime NOW = DateTimes.nowUtc();
  private static final byte[] CONTENT =
      StringUtils.toUtf8(StringUtils.format("%d,hello,world", NOW.getMillis()));

  static {
    INPUT_DATA_CONFIG = new OssInputDataConfig();
    INPUT_DATA_CONFIG.setMaxListingLength(MAX_LISTING_LENGTH);
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerdeWithUris() throws Exception
  {
    final OssInputSource withUris = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        null,
        null,
        null
    );
    final OssInputSource serdeWithUris = MAPPER.readValue(MAPPER.writeValueAsString(withUris), OssInputSource.class);
    Assert.assertEquals(withUris, serdeWithUris);
  }

  @Test
  public void testSerdeWithPrefixes() throws Exception
  {
    final OssInputSource withPrefixes = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null,
        null
    );
    final OssInputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), OssInputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
  }

  @Test
  public void testSerdeWithObjects() throws Exception
  {
    final OssInputSource withPrefixes = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        null,
        null,
        EXPECTED_LOCATION,
        null
    );
    final OssInputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), OssInputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
  }

  @Test
  public void testInputSourceUseDefaultPasswordWhenCloudConfigPropertiesWithoutCrediential()
  {
    OssClientConfig mockConfigPropertiesWithoutKeyAndSecret = EasyMock.createMock(OssClientConfig.class);
    EasyMock.reset(mockConfigPropertiesWithoutKeyAndSecret);
    EasyMock.expect(mockConfigPropertiesWithoutKeyAndSecret.isCredentialsConfigured())
            .andStubReturn(false);
    EasyMock.expect(mockConfigPropertiesWithoutKeyAndSecret.buildClient())
            .andReturn(OSSCLIENT);
    EasyMock.replay(mockConfigPropertiesWithoutKeyAndSecret);
    final OssInputSource withPrefixes = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        null,
        null,
        EXPECTED_LOCATION,
        mockConfigPropertiesWithoutKeyAndSecret
    );
    Assert.assertNotNull(withPrefixes);

    withPrefixes.createEntity(new CloudObjectLocation("bucket", "path"));
    EasyMock.verify(mockConfigPropertiesWithoutKeyAndSecret);
  }

  @Test
  public void testSerdeOssClientLazyInitializedWithCrediential() throws Exception
  {
    OssClientConfig clientConfig = EasyMock.createMock(OssClientConfig.class);
    EasyMock.replay(clientConfig);
    final OssInputSource withPrefixes = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        null,
        null,
        EXPECTED_LOCATION,
        CLOUD_CONFIG_PROPERTIES
    );
    final OssInputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), OssInputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
    EasyMock.verify(clientConfig);
  }

  @Test
  public void testSerdeOssClientLazyInitializedWithoutCrediential() throws Exception
  {
    OssClientConfig clientConfig = EasyMock.createMock(OssClientConfig.class);
    EasyMock.replay(clientConfig);
    final OssInputSource withPrefixes = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        null,
        null,
        EXPECTED_LOCATION,
        null
    );
    final OssInputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), OssInputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
    EasyMock.verify(clientConfig);
  }

  @Test
  public void testSerdeWithExtraEmptyLists() throws Exception
  {
    final OssInputSource withPrefixes = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        ImmutableList.of(),
        ImmutableList.of(),
        EXPECTED_LOCATION,
        null
    );
    final OssInputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), OssInputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
  }

  @Test
  public void testSerdeWithInvalidArgs()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        PREFIXES,
        EXPECTED_LOCATION,
        null
    );
  }

  @Test
  public void testSerdeWithOtherInvalidArgs()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        PREFIXES,
        ImmutableList.of(),
        null
    );
  }

  @Test
  public void testSerdeWithOtherOtherInvalidArgs()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        ImmutableList.of(),
        PREFIXES,
        EXPECTED_LOCATION,
        null
    );
  }

  @Test
  public void testWithUrisSplit()
  {
    OssInputSource inputSource = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        null,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null),
        null
    );

    Assert.assertEquals(EXPECTED_COORDS, splits.map(InputSplit::get).collect(Collectors.toList()));
  }

  @Test
  public void testWithPrefixesSplit()
  {
    EasyMock.reset(OSSCLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)), CONTENT);
    expectListObjects(PREFIXES.get(1), ImmutableList.of(EXPECTED_URIS.get(1)), CONTENT);
    EasyMock.replay(OSSCLIENT);

    OssInputSource inputSource = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null),
        new MaxSizeSplitHintSpec(1L) // set maxSplitSize to 1 so that each inputSplit has only one object
    );

    Assert.assertEquals(EXPECTED_COORDS, splits.map(InputSplit::get).collect(Collectors.toList()));
    EasyMock.verify(OSSCLIENT);
  }

  @Test
  public void testCreateSplitsWithSplitHintSpecRespectingHint()
  {
    EasyMock.reset(OSSCLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)), CONTENT);
    expectListObjects(PREFIXES.get(1), ImmutableList.of(EXPECTED_URIS.get(1)), CONTENT);
    EasyMock.replay(OSSCLIENT);

    OssInputSource inputSource = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null),
        new MaxSizeSplitHintSpec(CONTENT.length * 3L)
    );

    Assert.assertEquals(
        ImmutableList.of(EXPECTED_URIS.stream().map(CloudObjectLocation::new).collect(Collectors.toList())),
        splits.map(InputSplit::get).collect(Collectors.toList())
    );
    EasyMock.verify(OSSCLIENT);
  }

  @Test
  public void testCreateSplitsWithEmptyObjectsIteratingOnlyNonEmptyObjects()
  {
    EasyMock.reset(OSSCLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)), CONTENT);
    expectListObjects(PREFIXES.get(1), ImmutableList.of(EXPECTED_URIS.get(1)), new byte[0]);
    EasyMock.replay(OSSCLIENT);

    OssInputSource inputSource = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null),
        null
    );
    Assert.assertEquals(
        ImmutableList.of(ImmutableList.of(new CloudObjectLocation(EXPECTED_URIS.get(0)))),
        splits.map(InputSplit::get).collect(Collectors.toList())
    );
    EasyMock.verify(OSSCLIENT);
  }

  @Test
  public void testAccessDeniedWhileListingPrefix()
  {
    EasyMock.reset(OSSCLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)), CONTENT);
    expectListObjectsAndThrowAccessDenied(EXPECTED_URIS.get(1));
    EasyMock.replay(OSSCLIENT);

    OssInputSource inputSource = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        null,
        ImmutableList.of(PREFIXES.get(0), EXPECTED_URIS.get(1)),
        null,
        null
    );

    expectedException.expectMessage("Failed to get object summaries from aliyun OSS bucket[bar], prefix[foo/file2.csv]");
    expectedException.expectCause(
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("can't list that bucket"))
    );

    inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null),
        null
    ).collect(Collectors.toList());
  }

  @Test
  public void testReader() throws IOException
  {
    EasyMock.reset(OSSCLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)), CONTENT);
    expectListObjects(EXPECTED_URIS.get(1), ImmutableList.of(EXPECTED_URIS.get(1)), CONTENT);
    expectGetObject(EXPECTED_URIS.get(0));
    expectGetObject(EXPECTED_URIS.get(1));
    EasyMock.replay(OSSCLIENT);

    OssInputSource inputSource = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        null,
        ImmutableList.of(PREFIXES.get(0), EXPECTED_URIS.get(1)),
        null,
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

    EasyMock.verify(OSSCLIENT);
  }

  @Test
  public void testCompressedReader() throws IOException
  {
    EasyMock.reset(OSSCLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_COMPRESSED_URIS.get(0)), CONTENT);
    expectListObjects(EXPECTED_COMPRESSED_URIS.get(1), ImmutableList.of(EXPECTED_COMPRESSED_URIS.get(1)), CONTENT);
    expectGetObjectCompressed(EXPECTED_COMPRESSED_URIS.get(0));
    expectGetObjectCompressed(EXPECTED_COMPRESSED_URIS.get(1));
    EasyMock.replay(OSSCLIENT);

    OssInputSource inputSource = new OssInputSource(
        OSSCLIENT,
        INPUT_DATA_CONFIG,
        null,
        ImmutableList.of(PREFIXES.get(0), EXPECTED_COMPRESSED_URIS.get(1)),
        null,
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

    EasyMock.verify(OSSCLIENT);
  }

  private static void expectListObjects(URI prefix, List<URI> uris, byte[] content)
  {
    final ObjectListing result = new ObjectListing();
    result.setBucketName(prefix.getAuthority());
    result.setMaxKeys(uris.size());
    for (URI uri : uris) {
      final String bucket = uri.getAuthority();
      final String key = OssUtils.extractKey(uri);
      final OSSObjectSummary objectSummary = new OSSObjectSummary();
      objectSummary.setBucketName(bucket);
      objectSummary.setKey(key);
      objectSummary.setSize(content.length);
      result.getObjectSummaries().add(objectSummary);
    }

    EasyMock.expect(
        OSSCLIENT.listObjects(matchListObjectsRequest(prefix))
    ).andReturn(result).once();
  }

  private static void expectListObjectsAndThrowAccessDenied(final URI prefix)
  {
    OSSException boom = new OSSException("oh dang, you can't list that bucket friend");
    boom.setRawResponseError("403");
    EasyMock.expect(
        OSSCLIENT.listObjects(matchListObjectsRequest(prefix))
    ).andThrow(boom).once();
  }

  private static void expectGetObject(URI uri)
  {
    final String bucket = uri.getAuthority();
    final String key = OssUtils.extractKey(uri);

    OSSObject someObject = new OSSObject();
    someObject.setBucketName(bucket);
    someObject.setKey(key);
    someObject.setObjectContent(new ByteArrayInputStream(CONTENT));
    EasyMock.expect(OSSCLIENT.getObject(EasyMock.anyObject(GetObjectRequest.class))).andReturn(someObject).once();
  }

  private static void expectGetObjectCompressed(URI uri) throws IOException
  {
    final String bucket = uri.getAuthority();
    final String key = OssUtils.extractKey(uri);

    OSSObject someObject = new OSSObject();
    someObject.setBucketName(bucket);
    someObject.setKey(key);
    ByteArrayOutputStream gzipped = new ByteArrayOutputStream();
    CompressionUtils.gzip(new ByteArrayInputStream(CONTENT), gzipped);
    someObject.setObjectContent(new ByteArrayInputStream(gzipped.toByteArray()));
    EasyMock.expect(OSSCLIENT.getObject(EasyMock.anyObject(GetObjectRequest.class))).andReturn(someObject).once();
  }

  private static ListObjectsRequest matchListObjectsRequest(final URI prefixUri)
  {
    // Use an IArgumentMatcher to verify that the request has the correct bucket and prefix.
    EasyMock.reportMatcher(
        new IArgumentMatcher()
        {
          @Override
          public boolean matches(Object argument)
          {
            if (!(argument instanceof ListObjectsRequest)) {
              return false;
            }

            final ListObjectsRequest request = (ListObjectsRequest) argument;
            return prefixUri.getAuthority().equals(request.getBucketName())
                   && OssUtils.extractKey(prefixUri).equals(request.getPrefix());
          }

          @Override
          public void appendTo(StringBuffer buffer)
          {
            buffer.append("<request for prefix [").append(prefixUri).append("]>");
          }
        }
    );

    return null;
  }

  public static ObjectMapper createObjectMapper()
  {
    DruidModule baseModule = new TestOssModule();
    final Injector injector = Guice.createInjector(
        new ObjectMapperModule(),
        baseModule
    );
    final ObjectMapper baseMapper = injector.getInstance(ObjectMapper.class);

    baseModule.getJacksonModules().forEach(baseMapper::registerModule);
    return baseMapper;
  }

  public static class TestOssModule implements DruidModule
  {
    @Override
    public List<? extends Module> getJacksonModules()
    {
      // Deserializer is need for OSS even though it is injected.
      // See https://github.com/FasterXML/jackson-databind/issues/962.
      return ImmutableList.of(
          new SimpleModule()
              .addDeserializer(OSS.class, new ItemDeserializer<OSSClient>())
      );
    }

    @Override
    public void configure(Binder binder)
    {
    }

    @Provides
    public OSS getOssClient()
    {
      return OSSCLIENT;
    }
  }

  public static class ItemDeserializer<T> extends StdDeserializer<T>
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
    public T deserialize(JsonParser jp, DeserializationContext ctxt)
    {
      throw new UnsupportedOperationException();
    }
  }
}
