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

package org.apache.druid.segment.realtime.firehose;

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URI;

public class HttpFirehoseFactoryTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws IOException
  {
    final HttpInputSourceConfig inputSourceConfig = new HttpInputSourceConfig(null);
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(new Std().addValue(
        HttpInputSourceConfig.class,
        inputSourceConfig
    ));

    final DefaultPasswordProvider pwProvider = new DefaultPasswordProvider("testPassword");
    final HttpFirehoseFactory factory = new HttpFirehoseFactory(
        ImmutableList.of(URI.create("http://foo/bar"), URI.create("http://foo/bar2")),
        2048L,
        1024L,
        512L,
        100L,
        5,
        "testUser",
        pwProvider,
        inputSourceConfig
    );

    final HttpFirehoseFactory outputFact = mapper.readValue(
        mapper.writeValueAsString(factory),
        HttpFirehoseFactory.class
    );

    Assert.assertEquals(factory, outputFact);
  }

  @Test
  public void testConstructorAllowsOnlyDefaultProtocols()
  {
    new HttpFirehoseFactory(
        ImmutableList.of(URI.create("http:///")),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new HttpInputSourceConfig(null)
    );

    new HttpFirehoseFactory(
        ImmutableList.of(URI.create("https:///")),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new HttpInputSourceConfig(null)
    );

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Only [http, https] protocols are allowed");
    new HttpFirehoseFactory(
        ImmutableList.of(URI.create("my-protocol:///")),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new HttpInputSourceConfig(null)
    );
  }

  @Test
  public void testConstructorAllowsOnlyCustomProtocols()
  {
    final HttpInputSourceConfig customConfig = new HttpInputSourceConfig(ImmutableSet.of("druid"));
    new HttpFirehoseFactory(
        ImmutableList.of(URI.create("druid:///")),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        customConfig
    );

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Only [druid] protocols are allowed");
    new HttpFirehoseFactory(
        ImmutableList.of(URI.create("https:///")),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        customConfig
    );
  }
}
