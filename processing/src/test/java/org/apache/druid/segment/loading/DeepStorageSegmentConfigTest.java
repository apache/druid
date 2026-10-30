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
import org.apache.druid.utils.CompressionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

  @Test
  public void testUnsetFallsBackToCallerDefault()
  {
    final DeepStorageSegmentConfig config = new DeepStorageSegmentConfig();
    assertNull(config.getZip());
    assertNull(config.getCompressionFormat());

    // each DataSegmentPusher keeps its own default when druid.storage properties are not set
    assertTrue(config.isZip(true));
    assertFalse(config.isZip(false));

    assertEquals(CompressionUtils.Format.ZIP, config.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));
    assertEquals(CompressionUtils.Format.NONE, config.getEffectiveCompressionFormat(CompressionUtils.Format.NONE));
    assertEquals(CompressionUtils.Format.LZ4, config.getEffectiveCompressionFormat(CompressionUtils.Format.LZ4));
  }

  @Test
  public void testLegacyZipOverridesCallerDefault()
  {
    final DeepStorageSegmentConfig configZip = new DeepStorageSegmentConfig(true);
    assertTrue(configZip.isZip(false));
    assertEquals(CompressionUtils.Format.ZIP, configZip.getEffectiveCompressionFormat(CompressionUtils.Format.NONE));

    final DeepStorageSegmentConfig configNoZip = new DeepStorageSegmentConfig(false);
    assertFalse(configNoZip.isZip(true));
    assertEquals(CompressionUtils.Format.NONE, configNoZip.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));
  }

  @Test
  public void testExplicitCompressionFormatOverridesCallerDefault()
  {
    final DeepStorageSegmentConfig configZip = new DeepStorageSegmentConfig(null, CompressionUtils.Format.ZIP);
    assertEquals(CompressionUtils.Format.ZIP, configZip.getCompressionFormat());
    assertNull(configZip.getZip());
    assertTrue(configZip.isZip(false));
    assertEquals(CompressionUtils.Format.ZIP, configZip.getEffectiveCompressionFormat(CompressionUtils.Format.NONE));

    final DeepStorageSegmentConfig configNone = new DeepStorageSegmentConfig(null, CompressionUtils.Format.NONE);
    assertEquals(CompressionUtils.Format.NONE, configNone.getCompressionFormat());
    assertNull(configNone.getZip());
    assertFalse(configNone.isZip(true));
    assertEquals(CompressionUtils.Format.NONE, configNone.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));

    final DeepStorageSegmentConfig configLz4 = new DeepStorageSegmentConfig(null, CompressionUtils.Format.LZ4);
    assertEquals(CompressionUtils.Format.LZ4, configLz4.getCompressionFormat());
    assertNull(configLz4.getZip());
    assertFalse(configLz4.isZip(true));
    assertEquals(CompressionUtils.Format.LZ4, configLz4.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));
  }

  @Test
  public void testMatchingZipAndCompressionFormat()
  {
    final DeepStorageSegmentConfig configZip = new DeepStorageSegmentConfig(true, CompressionUtils.Format.ZIP);
    assertEquals(CompressionUtils.Format.ZIP, configZip.getEffectiveCompressionFormat(CompressionUtils.Format.NONE));
    assertTrue(configZip.isZip(false));

    final DeepStorageSegmentConfig configNone = new DeepStorageSegmentConfig(false, CompressionUtils.Format.NONE);
    assertEquals(CompressionUtils.Format.NONE, configNone.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));
    assertFalse(configNone.isZip(true));
  }

  @Test
  public void testConflictingConfigurationsThrowIAE()
  {
    // zip=false conflicts with compressionFormat=zip
    final IAE ex1 = assertThrows(
        IAE.class,
        () -> new DeepStorageSegmentConfig(false, CompressionUtils.Format.ZIP)
    );
    assertTrue(ex1.getMessage().contains("Conflicting compression configuration"));

    // zip=true conflicts with compressionFormat=none
    final IAE ex2 = assertThrows(
        IAE.class,
        () -> new DeepStorageSegmentConfig(true, CompressionUtils.Format.NONE)
    );
    assertTrue(ex2.getMessage().contains("Conflicting compression configuration"));

    // zip=true conflicts with compressionFormat=lz4
    final IAE ex3 = assertThrows(
        IAE.class,
        () -> new DeepStorageSegmentConfig(true, CompressionUtils.Format.LZ4)
    );
    assertTrue(ex3.getMessage().contains("Conflicting compression configuration"));

    // zip=false conflicts with compressionFormat=lz4
    final IAE ex4 = assertThrows(
        IAE.class,
        () -> new DeepStorageSegmentConfig(false, CompressionUtils.Format.LZ4)
    );
    assertTrue(ex4.getMessage().contains("Conflicting compression configuration"));
  }

  @Test
  public void testConfigurateFromPropertiesLegacyZip()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.storage.zip", "false");

    final DeepStorageSegmentConfig config = configurate(properties);
    assertEquals(Boolean.FALSE, config.getZip());
    assertNull(config.getCompressionFormat());
    assertFalse(config.isZip(true));
    assertEquals(CompressionUtils.Format.NONE, config.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));
  }

  @Test
  public void testConfigurateFromPropertiesCompressionFormat()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.storage.compressionFormat", "none");

    final DeepStorageSegmentConfig config = configurate(properties);
    assertNull(config.getZip());
    assertEquals(CompressionUtils.Format.NONE, config.getCompressionFormat());
    assertFalse(config.isZip(true));
    assertEquals(CompressionUtils.Format.NONE, config.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));
  }

  @Test
  public void testConfigurateFromPropertiesMatchingBoth()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.storage.zip", "true");
    properties.setProperty("druid.storage.compressionFormat", "zip");

    final DeepStorageSegmentConfig config = configurate(properties);
    assertEquals(Boolean.TRUE, config.getZip());
    assertEquals(CompressionUtils.Format.ZIP, config.getCompressionFormat());
    assertTrue(config.isZip(false));
    assertEquals(CompressionUtils.Format.ZIP, config.getEffectiveCompressionFormat(CompressionUtils.Format.NONE));
  }

  @Test
  public void testConfigurateFromPropertiesConflictingBothThrows()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.storage.zip", "false");
    properties.setProperty("druid.storage.compressionFormat", "zip");

    Assertions.assertThrows(
        RuntimeException.class,
        () -> configurate(properties)
    );
  }

  @Test
  public void testConfigurateIgnoresOtherStorageProperties()
  {
    // druid.storage is shared with the deep storage implementation's own config, so the properties belonging to it
    // must not upset this one
    final Properties properties = new Properties();
    properties.setProperty("druid.storage.type", "s3");
    properties.setProperty("druid.storage.bucket", "bucket");
    properties.setProperty("druid.storage.baseKey", "baseKey");

    final DeepStorageSegmentConfig config = configurate(properties);
    assertNull(config.getZip());
    assertNull(config.getCompressionFormat());
    assertTrue(config.isZip(true));
    assertEquals(CompressionUtils.Format.ZIP, config.getEffectiveCompressionFormat(CompressionUtils.Format.ZIP));
  }

  @Test
  public void testSerde() throws IOException
  {
    assertEquals(
        Boolean.TRUE,
        jsonMapper.readValue(jsonMapper.writeValueAsBytes(new DeepStorageSegmentConfig(true)), DeepStorageSegmentConfig.class)
                  .getZip()
    );
    assertNull(
        jsonMapper.readValue(jsonMapper.writeValueAsBytes(new DeepStorageSegmentConfig()), DeepStorageSegmentConfig.class)
                  .getZip()
    );

    final DeepStorageSegmentConfig configFormat = new DeepStorageSegmentConfig(null, CompressionUtils.Format.LZ4);
    final DeepStorageSegmentConfig deserializedFormat = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(configFormat),
        DeepStorageSegmentConfig.class
    );
    assertEquals(CompressionUtils.Format.LZ4, deserializedFormat.getCompressionFormat());
    assertNull(deserializedFormat.getZip());

    final DeepStorageSegmentConfig configBoth = new DeepStorageSegmentConfig(false, CompressionUtils.Format.NONE);
    final DeepStorageSegmentConfig deserializedBoth = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(configBoth),
        DeepStorageSegmentConfig.class
    );
    assertEquals(Boolean.FALSE, deserializedBoth.getZip());
    assertEquals(CompressionUtils.Format.NONE, deserializedBoth.getCompressionFormat());
  }

  private DeepStorageSegmentConfig configurate(Properties properties)
  {
    return new JsonConfigurator(jsonMapper, validator).configurate(properties, PROP_PREFIX, DeepStorageSegmentConfig.class);
  }
}
