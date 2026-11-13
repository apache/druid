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

package org.apache.druid.storage.s3;

import com.google.inject.ProvisionException;
import jakarta.validation.Validation;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

/**
 * Binding of {@link S3StorageConfig} to the runtime properties, as {@link S3StorageDruidModule} does.
 */
public class S3StorageConfigTest
{
  private static final String PREFIX = "druid.storage.";

  private final JsonConfigurator configurator = new JsonConfigurator(
      new DefaultObjectMapper(),
      Validation.buildDefaultValidatorFactory().getValidator()
  );

  @Test
  public void testValidTransferProperties()
  {
    final Properties properties = new Properties();
    properties.setProperty(PREFIX + "transfer.multipartUploadThreshold", "1024");

    final S3StorageConfig config = configurator.configurate(properties, PREFIX, S3StorageConfig.class);

    Assertions.assertEquals(1024L, config.getS3TransferConfig().getMultipartUploadThreshold());
  }

  @Test
  public void testInvalidTransferPropertyIsReportedByFullName()
  {
    final Properties properties = new Properties();
    properties.setProperty(PREFIX + "transfer.multipartUploadThreshold", "0");

    final ProvisionException e = Assertions.assertThrows(
        ProvisionException.class,
        () -> configurator.configurate(properties, PREFIX, S3StorageConfig.class)
    );

    Assertions.assertTrue(e.getMessage().contains(PREFIX + "transfer.multipartUploadThreshold - "), e.getMessage());
  }
}
