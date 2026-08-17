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
import org.apache.druid.metadata.PasswordProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.SslVerifyMode;

import java.io.IOException;

public class RedisCacheConfigTest
{
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
      Assertions.assertTrue(cache instanceof RedisClusterCache);
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

    final IAE exception = Assertions.assertThrows(IAE.class, () -> RedisCacheFactory.create(fromJson));
    Assertions.assertTrue(exception.getMessage().startsWith("Invalid redis cluster"));
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

    final IAE exception = Assertions.assertThrows(IAE.class, () -> RedisCacheFactory.create(fromJson));
    Assertions.assertTrue(exception.getMessage().startsWith("Invalid port"));
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

    final IAE exception = Assertions.assertThrows(IAE.class, () -> RedisCacheFactory.create(fromJson));
    Assertions.assertTrue(exception.getMessage().contains("Invalid port"));
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

    final IAE exception = Assertions.assertThrows(IAE.class, () -> RedisCacheFactory.create(fromJson));
    Assertions.assertTrue(exception.getMessage().contains("Invalid port"));
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

    final IAE exception = Assertions.assertThrows(IAE.class, () -> RedisCacheFactory.create(fromJson));
    Assertions.assertTrue(exception.getMessage().contains("no redis server"));
  }

  @Test
  public void testEnableTls() throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();

    RedisCacheConfig defaultConfig = mapper.readValue(
        "{\"host\": \"localhost\", \"port\": 6379}",
        RedisCacheConfig.class
    );
    Assertions.assertFalse(defaultConfig.getEnableTls());

    RedisCacheConfig tlsConfig = mapper.readValue(
        "{\"host\": \"localhost\", \"port\": 6379, \"enableTls\": true}",
        RedisCacheConfig.class
    );
    Assertions.assertTrue(tlsConfig.getEnableTls());
  }

  @Test
  public void testSkipTlsHostnameVerification() throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();

    RedisCacheConfig defaultConfig = mapper.readValue(
        "{\"host\": \"localhost\", \"port\": 6379}",
        RedisCacheConfig.class
    );
    Assertions.assertFalse(defaultConfig.getSkipTlsHostnameVerification());

    RedisCacheConfig skipConfig = mapper.readValue(
        "{\"host\": \"localhost\", \"port\": 6379, \"skipTlsHostnameVerification\": true}",
        RedisCacheConfig.class
    );
    Assertions.assertTrue(skipConfig.getSkipTlsHostnameVerification());
  }

  @Test
  public void testBuildClientConfig()
  {
    // TLS disabled: not SSL, no SSL options, database argument passed through, null password.
    JedisClientConfig plain = RedisCacheFactory.buildClientConfig(new RedisCacheConfig(), 3);
    Assertions.assertFalse(plain.isSsl());
    Assertions.assertNull(plain.getSslOptions());
    Assertions.assertEquals(3, plain.getDatabase());
    Assertions.assertNull(plain.getPassword());

    // TLS enabled with hostname verification (default) maps to SslVerifyMode.FULL, and a
    // non-null password is forwarded.
    RedisCacheConfig verifyConfig = new RedisCacheConfig()
    {
      @Override
      public boolean getEnableTls()
      {
        return true;
      }

      @Override
      public PasswordProvider getPassword()
      {
        return () -> "secret";
      }
    };
    JedisClientConfig verify = RedisCacheFactory.buildClientConfig(verifyConfig, 0);
    Assertions.assertTrue(verify.isSsl());
    Assertions.assertEquals(SslVerifyMode.FULL, verify.getSslOptions().getSslVerifyMode());
    Assertions.assertEquals("secret", verify.getPassword());

    // skipTlsHostnameVerification maps to SslVerifyMode.CA (chain verified, hostname skipped).
    RedisCacheConfig skipConfig = new RedisCacheConfig()
    {
      @Override
      public boolean getEnableTls()
      {
        return true;
      }

      @Override
      public boolean getSkipTlsHostnameVerification()
      {
        return true;
      }
    };
    Assertions.assertEquals(
        SslVerifyMode.CA,
        RedisCacheFactory.buildClientConfig(skipConfig, 0).getSslOptions().getSslVerifyMode()
    );
  }
}
