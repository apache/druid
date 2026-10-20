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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    // each DataSegmentPusher keeps its own default when druid.storage.zip is not set
    assertTrue(config.isZip(true));
    assertFalse(config.isZip(false));
  }

  @Test
  public void testSetOverridesCallerDefault()
  {
    assertTrue(new DeepStorageSegmentConfig(true).isZip(false));
    assertFalse(new DeepStorageSegmentConfig(false).isZip(true));
  }

  @Test
  public void testConfigurateFromProperties()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.storage.zip", "false");

    final DeepStorageSegmentConfig config = configurate(properties);
    assertEquals(Boolean.FALSE, config.getZip());
    assertFalse(config.isZip(true));
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
    assertTrue(config.isZip(true));
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
  }

  private DeepStorageSegmentConfig configurate(Properties properties)
  {
    return new JsonConfigurator(jsonMapper, validator).configurate(properties, PROP_PREFIX, DeepStorageSegmentConfig.class);
  }
}
