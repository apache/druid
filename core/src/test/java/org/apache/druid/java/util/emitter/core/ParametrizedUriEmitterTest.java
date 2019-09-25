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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.emitter.service.UnitEvent;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ParametrizedUriEmitterTest
{
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private MockHttpClient httpClient;
  private Lifecycle lifecycle;

  @Before
  public void setUp()
  {
    httpClient = new MockHttpClient();
  }

  @After
  public void tearDown()
  {
    if (lifecycle != null) {
      lifecycle.stop();
    }
  }

  private Emitter parametrizedEmmiter(String uriPattern) throws Exception
  {
    final Properties props = new Properties();
    props.setProperty("org.apache.druid.java.util.emitter.type", "parametrized");
    props.setProperty("org.apache.druid.java.util.emitter.recipientBaseUrlPattern", uriPattern);
    lifecycle = new Lifecycle();
    Emitter emitter = Emitters.create(props, httpClient, lifecycle);
    Assert.assertEquals(ParametrizedUriEmitter.class, emitter.getClass());
    lifecycle.start();
    return emitter;
  }

  @Test
  public void testParametrizedEmitterCreated() throws Exception
  {
    parametrizedEmmiter("http://example.com/");
  }

  @Test
  public void testEmitterWithFeedUriExtractor() throws Exception
  {
    Emitter emitter = parametrizedEmmiter("http://example.com/{feed}");
    final List<UnitEvent> events = Arrays.asList(
        new UnitEvent("test", 1),
        new UnitEvent("test", 2)
    );

    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          public ListenableFuture<Response> go(Request request) throws JsonProcessingException
          {
            Assert.assertEquals("http://example.com/test", request.getUrl());
            Assert.assertEquals(
                StringUtils.format(
                    "[%s,%s]\n",
                    JSON_MAPPER.writeValueAsString(events.get(0)),
                    JSON_MAPPER.writeValueAsString(events.get(1))
                ),
                StandardCharsets.UTF_8.decode(request.getByteBufferData().slice()).toString()
            );

            return GoHandlers.immediateFuture(EmitterTest.okResponse());
          }
        }.times(1)
    );

    for (UnitEvent event : events) {
      emitter.emit(event);
    }
    emitter.flush();
    Assert.assertTrue(httpClient.succeeded());
  }

  @Test
  public void testEmitterWithMultipleFeeds() throws Exception
  {
    Emitter emitter = parametrizedEmmiter("http://example.com/{feed}");
    final List<UnitEvent> events = Arrays.asList(
        new UnitEvent("test1", 1),
        new UnitEvent("test2", 2)
    );

    final Map<String, String> results = new HashMap<>();

    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          protected ListenableFuture<Response> go(Request request)
          {
            results.put(
                request.getUrl(),
                StandardCharsets.UTF_8.decode(request.getByteBufferData().slice()).toString()
            );
            return GoHandlers.immediateFuture(EmitterTest.okResponse());
          }
        }.times(2)
    );

    for (UnitEvent event : events) {
      emitter.emit(event);
    }
    emitter.flush();
    Assert.assertTrue(httpClient.succeeded());
    Map<String, String> expected = ImmutableMap.of(
        "http://example.com/test1", StringUtils.format("[%s]\n", JSON_MAPPER.writeValueAsString(events.get(0))),
        "http://example.com/test2", StringUtils.format("[%s]\n", JSON_MAPPER.writeValueAsString(events.get(1)))
    );
    Assert.assertEquals(expected, results);
  }

  @Test
  public void testEmitterWithParametrizedUriExtractor() throws Exception
  {
    Emitter emitter = parametrizedEmmiter("http://example.com/{key1}/{key2}");
    final List<UnitEvent> events = Arrays.asList(
        new UnitEvent("test", 1, ImmutableMap.of("key1", "val1", "key2", "val2")),
        new UnitEvent("test", 2, ImmutableMap.of("key1", "val1", "key2", "val2"))
    );

    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          protected ListenableFuture<Response> go(Request request) throws JsonProcessingException
          {
            Assert.assertEquals("http://example.com/val1/val2", request.getUrl());
            Assert.assertEquals(
                StringUtils.format(
                    "[%s,%s]\n",
                    JSON_MAPPER.writeValueAsString(events.get(0)),
                    JSON_MAPPER.writeValueAsString(events.get(1))
                ),
                StandardCharsets.UTF_8.decode(request.getByteBufferData().slice()).toString()
            );

            return GoHandlers.immediateFuture(EmitterTest.okResponse());
          }
        }.times(1)
    );

    for (UnitEvent event : events) {
      emitter.emit(event);
    }
    emitter.flush();
    Assert.assertTrue(httpClient.succeeded());
  }

  @Test
  public void failEmitMalformedEvent() throws Exception
  {
    Emitter emitter = parametrizedEmmiter("http://example.com/{keyNotSetInEvents}");
    Event event = new UnitEvent("test", 1);

    httpClient.setGoHandler(GoHandlers.failingHandler());

    try {
      emitter.emit(event);
      emitter.flush();
    }
    catch (IllegalArgumentException e) {
      Assert.assertEquals(
          e.getMessage(),
          StringUtils.format(
              "ParametrizedUriExtractor with pattern http://example.com/{keyNotSetInEvents} requires keyNotSetInEvents to be set in event, but found %s",
              event.toMap()
          )
      );
    }
  }
}
