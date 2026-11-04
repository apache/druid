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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.testing.junit.LoggerCaptureExtension;
import org.apache.druid.utils.CompressionUtils;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeepStorageSegmentConfigTest
{
  private static final String PROP_PREFIX = "druid.storage";

  @RegisterExtension
  public final LoggerCaptureExtension loggerCapture = new LoggerCaptureExtension(DeepStorageSegmentConfig.class);

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

  @Test
  public void testNeitherPropertyConfiguredUsesCallerDefault()
  {
    final DeepStorageSegmentConfig config = new DeepStorageSegmentConfig();
    assertNull(config.getCompressionFormat());

    // Caller-provided defaults are preserved when unset
    assertEquals(CompressionUtils.Format.ZIP, config.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));
    assertEquals(CompressionUtils.Format.NONE, config.getEffectiveCompressionFormat(CompressionUtils.Format.NONE));
    assertEquals(CompressionUtils.Format.LZ4, config.getEffectiveCompressionFormat(CompressionUtils.Format.LZ4));

    assertTrue(loggerCapture.getLogEvents().isEmpty());
  }

  @Test
  public void testLegacyZipTrueResolvesToZip()
  {
    final DeepStorageSegmentConfig config = new DeepStorageSegmentConfig(true, null);
    assertEquals(CompressionUtils.Format.ZIP, config.getCompressionFormat());
    assertEquals(CompressionUtils.Format.ZIP, config.getEffectiveCompressionFormat(CompressionUtils.Format.NONE));

    assertTrue(
        loggerCapture.getLogEvents().stream().anyMatch(
            e -> e.getLevel() == Level.WARN && e.getMessage().getFormattedMessage().contains("druid.storage.zip is deprecated")
        )
    );
  }

  @Test
  public void testLegacyZipFalseResolvesToNone()
  {
    final DeepStorageSegmentConfig config = new DeepStorageSegmentConfig(false, null);
    assertEquals(CompressionUtils.Format.NONE, config.getCompressionFormat());
    assertEquals(CompressionUtils.Format.NONE, config.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));

    assertTrue(
        loggerCapture.getLogEvents().stream().anyMatch(
            e -> e.getLevel() == Level.WARN && e.getMessage().getFormattedMessage().contains("druid.storage.zip is deprecated")
        )
    );
  }

  @Test
  public void testExplicitCompressionFormatResolvesCorrectly()
  {
    final DeepStorageSegmentConfig configZip = new DeepStorageSegmentConfig(null, CompressionUtils.Format.ZIP);
    assertEquals(CompressionUtils.Format.ZIP, configZip.getCompressionFormat());
    assertEquals(CompressionUtils.Format.ZIP, configZip.getEffectiveCompressionFormat(CompressionUtils.Format.NONE));

    final DeepStorageSegmentConfig configNone = new DeepStorageSegmentConfig(null, CompressionUtils.Format.NONE);
    assertEquals(CompressionUtils.Format.NONE, configNone.getCompressionFormat());
    assertEquals(CompressionUtils.Format.NONE, configNone.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));

    final DeepStorageSegmentConfig configLz4 = new DeepStorageSegmentConfig(null, CompressionUtils.Format.LZ4);
    assertEquals(CompressionUtils.Format.LZ4, configLz4.getCompressionFormat());
    assertEquals(CompressionUtils.Format.LZ4, configLz4.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));

    // When only compressionFormat is passed, no deprecation warning is logged
    assertTrue(loggerCapture.getLogEvents().isEmpty());
  }

  @Test
  public void testMatchingZipAndCompressionFormatTakesPrecedence()
  {
    final DeepStorageSegmentConfig configZip = new DeepStorageSegmentConfig(true, CompressionUtils.Format.ZIP);
    assertEquals(CompressionUtils.Format.ZIP, configZip.getCompressionFormat());
    assertEquals(CompressionUtils.Format.ZIP, configZip.getEffectiveCompressionFormat(CompressionUtils.Format.NONE));

    final DeepStorageSegmentConfig configNone = new DeepStorageSegmentConfig(false, CompressionUtils.Format.NONE);
    assertEquals(CompressionUtils.Format.NONE, configNone.getCompressionFormat());
    assertEquals(CompressionUtils.Format.NONE, configNone.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));

    // Legacy zip was provided, so deprecation warning was logged
    assertTrue(
        loggerCapture.getLogEvents().stream().anyMatch(
            e -> e.getLevel() == Level.WARN && e.getMessage().getFormattedMessage().contains("druid.storage.zip is deprecated")
        )
    );
  }

  @Test
  public void testConflictingZipAndCompressionFormatTakesPrecedenceWithoutException()
  {
    // zip=false but compressionFormat=zip -> compressionFormat (zip) wins
    final DeepStorageSegmentConfig config1 = new DeepStorageSegmentConfig(false, CompressionUtils.Format.ZIP);
    assertEquals(CompressionUtils.Format.ZIP, config1.getCompressionFormat());
    assertEquals(CompressionUtils.Format.ZIP, config1.getEffectiveCompressionFormat(CompressionUtils.Format.NONE));

    // zip=true but compressionFormat=none -> compressionFormat (none) wins
    final DeepStorageSegmentConfig config2 = new DeepStorageSegmentConfig(true, CompressionUtils.Format.NONE);
    assertEquals(CompressionUtils.Format.NONE, config2.getCompressionFormat());
    assertEquals(CompressionUtils.Format.NONE, config2.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));

    // zip=true but compressionFormat=lz4 -> compressionFormat (lz4) wins
    final DeepStorageSegmentConfig config3 = new DeepStorageSegmentConfig(true, CompressionUtils.Format.LZ4);
    assertEquals(CompressionUtils.Format.LZ4, config3.getCompressionFormat());
    assertEquals(CompressionUtils.Format.LZ4, config3.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));

    // zip=false but compressionFormat=lz4 -> compressionFormat (lz4) wins
    final DeepStorageSegmentConfig config4 = new DeepStorageSegmentConfig(false, CompressionUtils.Format.LZ4);
    assertEquals(CompressionUtils.Format.LZ4, config4.getCompressionFormat());
    assertEquals(CompressionUtils.Format.LZ4, config4.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));

    // Legacy zip was provided, so deprecation warning is logged
    assertTrue(
        loggerCapture.getLogEvents().stream().anyMatch(
            e -> e.getLevel() == Level.WARN && e.getMessage().getFormattedMessage().contains("druid.storage.zip is deprecated")
        )
    );
  }

  @Test
  public void testConfigurateFromPropertiesLegacyZip()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.storage.zip", "false");

    final DeepStorageSegmentConfig config = configurate(properties);
    assertEquals(CompressionUtils.Format.NONE, config.getCompressionFormat());
    assertEquals(CompressionUtils.Format.NONE, config.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));
  }

  @Test
  public void testConfigurateFromPropertiesCompressionFormat()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.storage.compressionFormat", "none");

    final DeepStorageSegmentConfig config = configurate(properties);
    assertEquals(CompressionUtils.Format.NONE, config.getCompressionFormat());
    assertEquals(CompressionUtils.Format.NONE, config.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));
  }

  @Test
  public void testConfigurateFromPropertiesMatchingBoth()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.storage.zip", "true");
    properties.setProperty("druid.storage.compressionFormat", "zip");

    final DeepStorageSegmentConfig config = configurate(properties);
    assertEquals(CompressionUtils.Format.ZIP, config.getCompressionFormat());
    assertEquals(CompressionUtils.Format.ZIP, config.getEffectiveCompressionFormat(CompressionUtils.Format.NONE));
  }

  @Test
  public void testConfigurateFromPropertiesConflictingBothTakesPrecedence()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.storage.zip", "false");
    properties.setProperty("druid.storage.compressionFormat", "zip");

    final DeepStorageSegmentConfig config = configurate(properties);
    assertEquals(CompressionUtils.Format.ZIP, config.getCompressionFormat());
    assertEquals(CompressionUtils.Format.ZIP, config.getEffectiveCompressionFormat(CompressionUtils.Format.NONE));
  }

  @Test
  public void testConfigurateIgnoresOtherStorageProperties()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.storage.type", "s3");
    properties.setProperty("druid.storage.bucket", "bucket");
    properties.setProperty("druid.storage.baseKey", "baseKey");

    final DeepStorageSegmentConfig config = configurate(properties);
    assertNull(config.getCompressionFormat());
    assertEquals(CompressionUtils.Format.ZIP, config.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));
  }

  @Test
  public void testSerializationExposesOnlyCompressionFormatNotZip() throws IOException
  {
    final DeepStorageSegmentConfig configLegacy = new DeepStorageSegmentConfig(true, null);
    final String jsonLegacy = jsonMapper.writeValueAsString(configLegacy);
    assertTrue(jsonLegacy.contains("\"compressionFormat\""));
    assertFalse(jsonLegacy.contains("\"zip\""));

    final DeepStorageSegmentConfig deserializedLegacy = jsonMapper.readValue(jsonLegacy, DeepStorageSegmentConfig.class);
    assertEquals(CompressionUtils.Format.ZIP, deserializedLegacy.getCompressionFormat());

    final DeepStorageSegmentConfig configUnset = new DeepStorageSegmentConfig();
    final String jsonUnset = jsonMapper.writeValueAsString(configUnset);
    assertFalse(jsonUnset.contains("\"zip\""));
    final DeepStorageSegmentConfig deserializedUnset = jsonMapper.readValue(jsonUnset, DeepStorageSegmentConfig.class);
    assertNull(deserializedUnset.getCompressionFormat());

    final DeepStorageSegmentConfig configLz4 = new DeepStorageSegmentConfig(null, CompressionUtils.Format.LZ4);
    final String jsonLz4 = jsonMapper.writeValueAsString(configLz4);
    assertTrue(jsonLz4.contains("\"compressionFormat\""));
    assertFalse(jsonLz4.contains("\"zip\""));
    final DeepStorageSegmentConfig deserializedLz4 = jsonMapper.readValue(jsonLz4, DeepStorageSegmentConfig.class);
    assertEquals(CompressionUtils.Format.LZ4, deserializedLz4.getCompressionFormat());
  }

  @Test
  public void testGetEffectiveCompressionFormatWithSupportedFormats()
  {
    final DeepStorageSegmentConfig config = new DeepStorageSegmentConfig(null, CompressionUtils.Format.ZIP);
    assertEquals(
        CompressionUtils.Format.ZIP,
        config.getEffectiveCompressionFormat(
            CompressionUtils.Format.NONE,
            CompressionUtils.Format.ZIP,
            CompressionUtils.Format.NONE
        )
    );
  }

  @Test
  public void testGetEffectiveCompressionFormatUnsupportedThrowsIAE()
  {
    final DeepStorageSegmentConfig config = new DeepStorageSegmentConfig(null, CompressionUtils.Format.LZ4);
    final IAE exception = assertThrows(
        IAE.class,
        () -> config.getEffectiveCompressionFormat(
            CompressionUtils.Format.ZIP,
            CompressionUtils.Format.ZIP,
            CompressionUtils.Format.NONE
        )
    );
    assertTrue(exception.getMessage().contains("Compression format [LZ4] is not supported"));
    assertTrue(exception.getMessage().contains("[ZIP, NONE]"));
  }

  @Test
  public void testGetEffectiveCompressionFormatDefaultUnsupportedThrowsIAE()
  {
    final DeepStorageSegmentConfig config = new DeepStorageSegmentConfig();
    final IAE exception = assertThrows(
        IAE.class,
        () -> config.getEffectiveCompressionFormat(
            CompressionUtils.Format.LZ4,
            CompressionUtils.Format.ZIP,
            CompressionUtils.Format.NONE
        )
    );
    assertTrue(exception.getMessage().contains("Compression format [LZ4] is not supported"));
    assertTrue(exception.getMessage().contains("[ZIP, NONE]"));
  }

  @Test
  public void testGetEffectiveCompressionFormatNullDefaultThrowsNPE()
  {
    final DeepStorageSegmentConfig config = new DeepStorageSegmentConfig();
    assertThrows(
        NullPointerException.class,
        () -> config.getEffectiveCompressionFormat(null, CompressionUtils.Format.ZIP)
    );
  }

  private DeepStorageSegmentConfig configurate(Properties properties)
  {
    return new JsonConfigurator(jsonMapper, validator).configurate(properties, PROP_PREFIX, DeepStorageSegmentConfig.class);
  }
}
