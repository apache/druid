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

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HttpFirehoseFactoryTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final HttpFirehoseFactory factory = new HttpFirehoseFactory(
        ImmutableList.of(URI.create("http://foo/bar"), URI.create("http://foo/bar2")),
        2048L,
        1024L,
        512L,
        100L,
        5
    );

    final HttpFirehoseFactory outputFact = mapper.readValue(
        mapper.writeValueAsString(factory),
        HttpFirehoseFactory.class
    );

    Assert.assertEquals(factory, outputFact);
  }
}
