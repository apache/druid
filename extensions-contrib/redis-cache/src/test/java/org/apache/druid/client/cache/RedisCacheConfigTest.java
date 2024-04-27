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

package org.apache.druid.client.cache;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fppt.jedismock.RedisServer;
import com.github.fppt.jedismock.server.ServiceOptions;
import org.apache.druid.java.util.common.IAE;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class RedisCacheConfigTest
{
  @FunctionalInterface
  interface MessageMatcher
  {
    boolean match(String input);
  }

  static class StartWithMatcher implements MessageMatcher
  {
    private String expected;

    public StartWithMatcher(String expected)
    {
      this.expected = expected;
    }

    @Override
    public boolean match(String input)
    {
      return input.startsWith(expected);
    }
  }

  static class ContainsMatcher implements MessageMatcher
  {
    private String expected;

    public ContainsMatcher(String expected)
    {
      this.expected = expected;
    }

    @Override
    public boolean match(String input)
    {
      return input.contains(expected);
    }
  }

  static class ExceptionMatcher implements Matcher
  {
    private MessageMatcher messageMatcher;
    private Class<? extends Throwable> exceptionClass;

    public ExceptionMatcher(Class<? extends Throwable> exceptionClass, MessageMatcher exceptionMessageMatcher)
    {
      this.exceptionClass = exceptionClass;
      this.messageMatcher = exceptionMessageMatcher;
    }

    @Override
    public boolean matches(Object item)
    {
      if (!(item.getClass().equals(exceptionClass))) {
        return false;
      }

      return this.messageMatcher.match(((Throwable) item).getMessage());
    }

    @Override
    public void describeMismatch(Object item, Description mismatchDescription)
    {
    }

    @Override
    public void _dont_implement_Matcher___instead_extend_BaseMatcher_()
    {
    }

    @Override
    public void describeTo(Description description)
    {
    }
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testClusterPriority() throws IOException
  {
    ServiceOptions options = ServiceOptions.defaultOptions().withClusterModeEnabled();
    RedisServer server = RedisServer.newRedisServer().setOptions(options).start();

    ObjectMapper mapper = new ObjectMapper();
    RedisCacheConfig fromJson = mapper.readValue("{\"expiration\": 1000,"
                                                 + "\"cluster\": {"
                                                 + "\"nodes\": \"" + server.getHost() + ":" + server.getBindPort() + "\""
                                                 + "},"
                                                 + "\"host\": \"" + server.getHost() + "\","
                                                 + "\"port\": " + server.getBindPort()
                                                 + "}", RedisCacheConfig.class);

    try (Cache cache = RedisCacheFactory.create(fromJson)) {
      Assert.assertTrue(cache instanceof RedisClusterCache);
    }
    finally {
      server.stop();
    }
  }

  @Test
  public void testClusterInvalidNode() throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    RedisCacheConfig fromJson = mapper.readValue(
        "{\"expiration\": 1000,"
        + "\"cluster\": {"
        + "\"nodes\": \"127.0.0.1\"" //<===Invalid Node
        + "}"
        + "}",
        RedisCacheConfig.class
    );

    expectedException.expect(new ExceptionMatcher(
        IAE.class,
        new StartWithMatcher("Invalid redis cluster")
    ));
    RedisCacheFactory.create(fromJson);
  }

  @Test
  public void testClusterLackOfPort() throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    RedisCacheConfig fromJson = mapper.readValue(
        "{\"expiration\":1000,"
        + "\"cluster\": {"
        + "\"nodes\": \"127.0.0.1:\""
        + "}"
        + "}",
        RedisCacheConfig.class
    );

    expectedException.expect(new ExceptionMatcher(
        IAE.class,
        new StartWithMatcher("Invalid port")
    ));
    RedisCacheFactory.create(fromJson);
  }

  @Test
  public void testInvalidClusterNodePort0() throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    RedisCacheConfig fromJson = mapper.readValue(
        "{\"expiration\": 1000,"
        + "\"cluster\": {"
        + "\"nodes\": \"127.0.0.1:0\"" //<===Invalid Port
        + "}"
        + "}",
        RedisCacheConfig.class
    );

    expectedException.expect(new ExceptionMatcher(
        IAE.class,
        new ContainsMatcher("Invalid port")
    ));
    RedisCacheFactory.create(fromJson);
  }

  @Test
  public void testInvalidClusterNodePort65536() throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    RedisCacheConfig fromJson = mapper.readValue(
        "{\"expiration\": 1000,"
        + "\"cluster\": {"
        + "\"nodes\": \"127.0.0.1:65536\"" //<===Invalid Port
        + "}"
        + "}",
        RedisCacheConfig.class
    );

    expectedException.expect(new ExceptionMatcher(
        IAE.class,
        new ContainsMatcher("Invalid port")
    ));
    RedisCacheFactory.create(fromJson);
  }

  @Test
  public void testNoClusterAndHost() throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    RedisCacheConfig fromJson = mapper.readValue(
        "{\"expiration\": 1000"
        + "}",
        RedisCacheConfig.class
    );

    expectedException.expect(new ExceptionMatcher(
        IAE.class,
        new ContainsMatcher("no redis server")
    ));
    RedisCacheFactory.create(fromJson);
  }
}
