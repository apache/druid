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
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
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
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class S3InputSourceTest
{
  private static final AmazonS3Client S3_CLIENT = EasyMock.createNiceMock(AmazonS3Client.class);
  private static final ServerSideEncryptingAmazonS3 SERVICE = new ServerSideEncryptingAmazonS3(
      S3_CLIENT,
      new NoopServerSideEncryption()
  );

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = createS3ObjectMapper();

    final List<URI> uris = Arrays.asList(
        new URI("s3://foo/bar/file.gz"),
        new URI("s3://bar/foo/file2.gz")
    );

    final List<URI> prefixes = Arrays.asList(
        new URI("s3://foo/bar"),
        new URI("s3://bar/foo")
    );

    final S3InputSource withUris = new S3InputSource(SERVICE, uris, null);
    final S3InputSource serdeWithUris = mapper.readValue(mapper.writeValueAsString(withUris), S3InputSource.class);
    Assert.assertEquals(withUris, serdeWithUris);

    final S3InputSource withPrefixes = new S3InputSource(SERVICE, null, prefixes);
    final S3InputSource serdeWithPrefixes = mapper.readValue(mapper.writeValueAsString(withPrefixes), S3InputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
  }

  @Test
  public void testWithUrisSplit() throws IOException
  {
    final List<URI> uris = Arrays.asList(
        URI.create("s3://foo/bar/file.gz"),
        URI.create("s3://bar/foo/file2.gz")
    );

    S3InputSource inputSource = new S3InputSource(SERVICE, uris, null);

    Stream<InputSplit<URI>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null),
        null
    );

    Assert.assertEquals(uris, splits.map(InputSplit::get).collect(Collectors.toList()));
  }

  @Test
  public void testWithPrefixesSplit() throws IOException
  {
    final List<URI> prefixes = Arrays.asList(
        URI.create("s3://foo/bar"),
        URI.create("s3://bar/foo")
    );

    final List<URI> expectedUris = Arrays.asList(
        URI.create("s3://foo/bar/file.gz"),
        URI.create("s3://bar/foo/file2.gz")
    );
    addExpectedPrefixObjects(SERVICE, URI.create("s3://foo/bar"), ImmutableList.of(expectedUris.get(0)));
    addExpectedPrefixObjects(SERVICE, URI.create("s3://bar/foo"), ImmutableList.of(expectedUris.get(1)));
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(SERVICE, null, prefixes);

    Stream<InputSplit<URI>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null),
        null
    );

    Assert.assertEquals(expectedUris, splits.map(InputSplit::get).collect(Collectors.toList()));
  }

  private static void addExpectedPrefixObjects(ServerSideEncryptingAmazonS3 service, URI prefix, List<URI> uris)
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
    EasyMock.expect(service.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class))).andReturn(result).once();
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
    public ItemDeserializer()
    {
      this(null);
    }

    public ItemDeserializer(Class<?> vc)
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
