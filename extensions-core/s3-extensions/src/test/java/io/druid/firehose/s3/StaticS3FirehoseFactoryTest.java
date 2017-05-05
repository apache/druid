/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.firehose.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.easymock.EasyMock;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 */
public class StaticS3FirehoseFactoryTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    final List<URI> uris = Arrays.asList(
        new URI("s3://foo/bar/file.gz"),
        new URI("s3://bar/foo/file2.gz")
    );

    TestStaticS3FirehoseFactory factory = new TestStaticS3FirehoseFactory(
        uris
    );

    TestStaticS3FirehoseFactory outputFact = mapper.readValue(
        mapper.writeValueAsString(factory),
        TestStaticS3FirehoseFactory.class
    );

    Assert.assertEquals(factory, outputFact);
    Assert.assertEquals(uris, outputFact.getUris());
  }

  // This class is a workaround for the injectable value that StaticS3FirehoseFactory requires
  private static class TestStaticS3FirehoseFactory extends StaticS3FirehoseFactory
  {
    @JsonCreator
    public TestStaticS3FirehoseFactory(
        @JsonProperty("uris") List<URI> uris
    )
    {
      super(EasyMock.niceMock(RestS3Service.class), uris, null, null, null, null, null, null);
    }
  }
}
