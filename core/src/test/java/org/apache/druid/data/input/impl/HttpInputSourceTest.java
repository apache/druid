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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

public class HttpInputSourceTest
{
  @Test
  public void testSerde() throws IOException
  {
    HttpInputSourceConfig httpInputSourceConfig = new HttpInputSourceConfig(
        null,
        null
    );
    final ObjectMapper mapper = new ObjectMapper();
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
        HttpInputSourceConfig.class,
        httpInputSourceConfig
    ));
    final HttpInputSource source = new HttpInputSource(
        ImmutableList.of(URI.create("http://test.com/http-test")),
        "myName",
        new DefaultPasswordProvider("myPassword"),
        httpInputSourceConfig
    );
    final byte[] json = mapper.writeValueAsBytes(source);
    final HttpInputSource fromJson = (HttpInputSource) mapper.readValue(json, InputSource.class);
    Assert.assertEquals(source, fromJson);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDenyListDomainThrowsException()
  {
    new HttpInputSource(
        ImmutableList.of(URI.create("http://deny.com/http-test")),
        "myName",
        new DefaultPasswordProvider("myPassword"),
        new HttpInputSourceConfig(null, Collections.singletonList("deny.com"))
    );
  }

  @Test
  public void testDenyListDomainNoMatch()
  {
    new HttpInputSource(
        ImmutableList.of(URI.create("http://allow.com/http-test")),
        "myName",
        new DefaultPasswordProvider("myPassword"),
        new HttpInputSourceConfig(null, Collections.singletonList("deny.com"))
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAllowListDomainThrowsException()
  {
    new HttpInputSource(
        ImmutableList.of(URI.create("http://deny.com/http-test")),
        "myName",
        new DefaultPasswordProvider("myPassword"),
        new HttpInputSourceConfig(Collections.singletonList("allow.com"), null)
    );
  }

  @Test
  public void testAllowListDomainMatch()
  {
    new HttpInputSource(
        ImmutableList.of(URI.create("http://allow.com/http-test")),
        "myName",
        new DefaultPasswordProvider("myPassword"),
        new HttpInputSourceConfig(Collections.singletonList("allow.com"), null)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyAllowListDomainMatch()
  {
    new HttpInputSource(
        ImmutableList.of(URI.create("http://allow.com/http-test")),
        "myName",
        new DefaultPasswordProvider("myPassword"),
        new HttpInputSourceConfig(Collections.emptyList(), null)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCannotSetBothAllowAndDenyList()
  {
    new HttpInputSourceConfig(Collections.singletonList("allow.com"), Collections.singletonList("deny.com"));
  }
}
