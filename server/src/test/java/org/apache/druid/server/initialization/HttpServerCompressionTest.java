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


package org.apache.druid.server.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.ProvisionException;
import jakarta.validation.Validation;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.http.client.HttpClientConfig.CompressionCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class HttpServerCompressionTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final String PREFIX = "druid.server.http";

  @Test
  public void testDefaults()
  {
    final HttpServerCompression config = configure(new Properties());
    Assertions.assertEquals(ImmutableList.of(CompressionCodec.ZSTD, CompressionCodec.GZIP), config.getCodecs());
    Assertions.assertEquals(ImmutableList.of("zstd", "gzip"), config.getCodecNames());
    Assertions.assertEquals(1, config.getZstd().level());
    Assertions.assertEquals(-1, config.getGzip().levelOrDefault());
    Assertions.assertEquals(4096, config.getGzip().inflateBufferSizeOrDefault());
    Assertions.assertEquals(HttpServerCompression.DEFAULT, config);
  }

  @Test
  public void testConfigureAll()
  {
    final Properties props = new Properties();
    props.setProperty(PREFIX + ".compression.codecs", "[\"gzip\", \"ZSTD\"]");
    props.setProperty(PREFIX + ".compression.zstd.level", "19");
    props.setProperty(PREFIX + ".compression.gzip.level", "9");
    props.setProperty(PREFIX + ".compression.gzip.inflateBufferSize", "0");
    final HttpServerCompression config = configure(props);
    Assertions.assertEquals(ImmutableList.of(CompressionCodec.GZIP, CompressionCodec.ZSTD), config.getCodecs());
    Assertions.assertEquals(19, config.getZstd().level());
    Assertions.assertEquals(9, config.getGzip().levelOrDefault());
    Assertions.assertEquals(0, config.getGzip().inflateBufferSizeOrDefault());
  }

  @Test
  public void testCompressionDisabled()
  {
    final Properties props = new Properties();
    props.setProperty(PREFIX + ".compression.codecs", "[]");
    Assertions.assertEquals(ImmutableList.of(), configure(props).getCodecs());
  }

  @Test
  public void testGzipLevelOverridesDeprecatedCompressionLevel()
  {
    final Properties props = new Properties();
    props.setProperty(PREFIX + ".compressionLevel", "3");
    props.setProperty(PREFIX + ".compression.gzip.level", "7");
    Assertions.assertEquals(7, configure(props).getGzip().levelOrDefault());
  }

  @Test
  public void testGzipInflateBufferSizeOverridesDeprecatedInflateBufferSize()
  {
    final Properties props = new Properties();
    props.setProperty(PREFIX + ".inflateBufferSize", "0");
    props.setProperty(PREFIX + ".compression.gzip.inflateBufferSize", "8192");
    Assertions.assertEquals(8192, configure(props).getGzip().inflateBufferSizeOrDefault());
  }

  @Test
  public void testDeprecatedPropertiesApplyTogether()
  {
    final Properties props = new Properties();
    props.setProperty(PREFIX + ".compressionLevel", "3");
    props.setProperty(PREFIX + ".inflateBufferSize", "2048");
    final HttpServerCompression config = configure(props);
    Assertions.assertEquals(3, config.getGzip().levelOrDefault());
    Assertions.assertEquals(2048, config.getGzip().inflateBufferSizeOrDefault());
  }

  @Test
  public void testInvalidInflateBufferSize()
  {
    final Properties props = new Properties();
    props.setProperty(PREFIX + ".compression.gzip.inflateBufferSize", "-1");
    assertConfigError(
        props,
        "Invalid gzip inflateBufferSize[-1]. It must be 0 or greater, where 0 disables request decompression."
    );
  }

  @Test
  public void testInvalidDeprecatedCompressionLevel()
  {
    final Properties props = new Properties();
    props.setProperty(PREFIX + ".compressionLevel", "10");
    assertConfigError(props, "compressionLevel");
  }

  @Test
  public void testInvalidCodec()
  {
    final Properties unknown = new Properties();
    unknown.setProperty(PREFIX + ".compression.codecs", "[\"lz4\"]");
    assertConfigError(unknown, "Invalid compression codec[lz4].");

    final Properties identity = new Properties();
    identity.setProperty(PREFIX + ".compression.codecs", "[\"Identity\"]");
    assertConfigError(
        identity,
        "Invalid HTTP server compression codec[Identity]. Set codecs to empty list `[]` to disable response compression."
    );
  }

  @Test
  public void testDuplicateCodecsAreIgnored()
  {
    final Properties props = new Properties();
    props.setProperty(PREFIX + ".compression.codecs", "[\"gzip\", \"zstd\", \"GZIP\"]");
    Assertions.assertEquals(ImmutableList.of(CompressionCodec.GZIP, CompressionCodec.ZSTD), configure(props).getCodecs());
  }

  @Test
  public void testInvalidZstdLevel()
  {
    for (String level : new String[]{"0", "20", "-1"}) {
      final Properties props = new Properties();
      props.setProperty(PREFIX + ".compression.zstd.level", level);
      assertConfigError(props, "Invalid zstd compression level[" + level + "]. It must be between 1 and 19.");
    }
  }

  @Test
  public void testInvalidGzipLevel()
  {
    for (String level : new String[]{"10", "-2"}) {
      final Properties props = new Properties();
      props.setProperty(PREFIX + ".compression.gzip.level", level);
      assertConfigError(
          props,
          "Invalid gzip compression level[" + level + "]. It must be between 0 and 9, or -1 for the default level."
      );
    }
  }

  @Test
  public void testSerde() throws Exception
  {
    final HttpServerCompression config = new HttpServerCompression(
        ImmutableList.of("gzip"),
        new HttpServerCompression.Zstd(5),
        null
    );
    final String json = MAPPER.writeValueAsString(config);
    Assertions.assertEquals("{\"codecs\":[\"gzip\"],\"zstd\":{\"level\":5},\"gzip\":{\"level\":null,\"inflateBufferSize\":null}}", json);
    Assertions.assertEquals(config, MAPPER.readValue(json, HttpServerCompression.class));
  }

  @Test
  public void testEmptyCodecSettingsUseDefaultLevels() throws Exception
  {
    final HttpServerCompression config = MAPPER.readValue(
        "{\"zstd\":{},\"gzip\":{\"level\":null}}",
        HttpServerCompression.class
    );
    Assertions.assertEquals(HttpServerCompression.DEFAULT, config);
    Assertions.assertEquals(HttpServerCompression.Zstd.DEFAULT_LEVEL, config.getZstd().level());
    Assertions.assertEquals(HttpServerCompression.Gzip.DEFAULT_LEVEL, config.getGzip().levelOrDefault());
  }

  @Test
  public void testServerConfigSerdeKeepsDeprecatedLevel() throws Exception
  {
    final Properties props = new Properties();
    props.setProperty(PREFIX + ".compressionLevel", "4");
    final ServerConfig serverConfig = configureServer(props);
    final ServerConfig roundTripped = MAPPER.readValue(MAPPER.writeValueAsString(serverConfig), ServerConfig.class);
    Assertions.assertEquals(4, roundTripped.getHttpServerCompression().getGzip().levelOrDefault());
    Assertions.assertEquals(serverConfig, roundTripped);
  }

  private static HttpServerCompression configure(final Properties props)
  {
    return configureServer(props).getHttpServerCompression();
  }

  private static ServerConfig configureServer(final Properties props)
  {
    final JsonConfigurator configurator = new JsonConfigurator(
        MAPPER,
        Validation.buildDefaultValidatorFactory().getValidator()
    );
    return configurator.configurate(props, PREFIX, ServerConfig.class);
  }

  private static void assertConfigError(final Properties props, final String expectedMessage)
  {
    final ProvisionException e = Assertions.assertThrows(ProvisionException.class, () -> configureServer(props));
    Assertions.assertTrue(
        e.getMessage().contains(expectedMessage),
        "Expected message to contain [" + expectedMessage + "] but was: " + e.getMessage()
    );
  }
}
