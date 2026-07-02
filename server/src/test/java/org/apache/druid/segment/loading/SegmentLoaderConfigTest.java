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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.ProvisionException;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.utils.RuntimeInfo;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Validation;
import java.util.Properties;

public class SegmentLoaderConfigTest
{
  private static final String CONFIG_BASE = "druid.segmentCache";

  @Test
  public void testSetVirtualStorage()
  {
    final SegmentLoaderConfig config = new SegmentLoaderConfig();

    // Verify default values
    Assert.assertFalse(config.isVirtualStorage());
    Assert.assertFalse(config.isVirtualStorageEphemeral());

    // Set both to true
    config.setVirtualStorage(true).setVirtualStorageIsEphemeral(true);

    // Verify both fields are set
    Assert.assertTrue(config.isVirtualStorage());
    Assert.assertTrue(config.isVirtualStorageEphemeral());
  }

  @Test
  public void testCoalesceConfigDefaults()
  {
    final SegmentLoaderConfig config = bind(new Properties());
    Assert.assertEquals(PartialSegmentFileMapperV10.CoalesceConfig.DEFAULT, config.getVirtualStorageCoalesceConfig());
  }

  @Test
  public void testCoalesceConfigCustomValid()
  {
    final Properties props = new Properties();
    props.setProperty(CONFIG_BASE + ".virtualStorageCoalesceMaxGapBytes", "2048");
    props.setProperty(CONFIG_BASE + ".virtualStorageCoalesceMaxChunkBytes", "4096");
    final PartialSegmentFileMapperV10.CoalesceConfig coalesce = bind(props).getVirtualStorageCoalesceConfig();
    Assert.assertEquals(2048L, coalesce.maxGapBytes());
    Assert.assertEquals(4096L, coalesce.maxChunkBytes());
  }

  @Test
  public void testNegativeMaxGapBytesFailsAtBinding()
  {
    final Properties props = new Properties();
    props.setProperty(CONFIG_BASE + ".virtualStorageCoalesceMaxGapBytes", "-1");
    final ProvisionException e = Assert.assertThrows(ProvisionException.class, () -> bind(props));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("virtualStorageCoalesceMaxGapBytes"));
  }

  @Test
  public void testZeroMaxChunkBytesFailsAtBinding()
  {
    final Properties props = new Properties();
    props.setProperty(CONFIG_BASE + ".virtualStorageCoalesceMaxChunkBytes", "0");
    final ProvisionException e = Assert.assertThrows(ProvisionException.class, () -> bind(props));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("virtualStorageCoalesceMaxChunkBytes"));
  }

  /**
   * Bind {@code properties} into a {@link SegmentLoaderConfig} through {@link JsonConfigurator}, the same parse + JSR-303
   * validate path Druid runs at startup, so an invalid value surfaces as a {@link ProvisionException} here just as it
   * would when a process boots.
   */
  private static SegmentLoaderConfig bind(Properties properties)
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(new InjectableValues.Std().addValue(RuntimeInfo.class, new RuntimeInfo()));
    final JsonConfigurator configurator = new JsonConfigurator(
        mapper,
        Validation.buildDefaultValidatorFactory().getValidator()
    );
    return configurator.configurate(properties, CONFIG_BASE, SegmentLoaderConfig.class);
  }
}
