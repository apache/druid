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

package org.apache.druid.firehose.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.s3.S3InputSourceTest;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 */
public class StaticS3FirehoseFactoryTest
{
  private static final AmazonS3Client S3_CLIENT = EasyMock.createNiceMock(AmazonS3Client.class);
  private static final ServerSideEncryptingAmazonS3 SERVICE = new ServerSideEncryptingAmazonS3(
      S3_CLIENT,
      new NoopServerSideEncryption()
  );

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = S3InputSourceTest.createS3ObjectMapper();

    final List<URI> uris = Arrays.asList(
        new URI("s3://foo/bar/file.gz"),
        new URI("s3://bar/foo/file2.gz")
    );

    final StaticS3FirehoseFactory factory = new StaticS3FirehoseFactory(
        SERVICE,
        uris,
        null,
        2048L,
        1024L,
        512L,
        100L,
        5
    );

    final StaticS3FirehoseFactory outputFact = mapper.readValue(
        mapper.writeValueAsString(factory),
        StaticS3FirehoseFactory.class
    );

    Assert.assertEquals(factory, outputFact);
  }

  @Test
  public void testWithSplit() throws IOException
  {
    final List<URI> uris = Arrays.asList(
        URI.create("s3://foo/bar/file.gz"),
        URI.create("s3://bar/foo/file2.gz")
    );
    uris.sort(Comparator.comparing(URI::toString));

    final StaticS3FirehoseFactory factory = new StaticS3FirehoseFactory(
        SERVICE,
        uris,
        null,
        2048L,
        1024L,
        512L,
        100L,
        5
    );
    final List<FiniteFirehoseFactory<StringInputRowParser, URI>> subFactories = factory
        .getSplits(null)
        .map(factory::withSplit)
        .sorted(Comparator.comparing(eachFactory -> {
          final StaticS3FirehoseFactory staticS3FirehoseFactory = (StaticS3FirehoseFactory) eachFactory;
          return staticS3FirehoseFactory.getUris().toString();
        }))
        .collect(Collectors.toList());

    Assert.assertEquals(uris.size(), subFactories.size());
    for (int i = 0; i < uris.size(); i++) {
      final StaticS3FirehoseFactory staticS3FirehoseFactory = (StaticS3FirehoseFactory) subFactories.get(i);
      final List<URI> subFactoryUris = staticS3FirehoseFactory.getUris();
      Assert.assertEquals(1, subFactoryUris.size());
      Assert.assertEquals(uris.get(i), subFactoryUris.get(0));
    }
  }
}
