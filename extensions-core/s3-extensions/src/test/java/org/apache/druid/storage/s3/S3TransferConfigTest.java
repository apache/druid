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

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.druid.storage.s3.output.S3OutputConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class S3TransferConfigTest
{
  private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

  @Test
  public void testPartSizeBelowTheS3MinimumIsRejected()
  {
    final S3TransferConfig config = new S3TransferConfig();
    config.setMinimumUploadPartSize(S3OutputConfig.S3_MULTIPART_UPLOAD_MIN_PART_SIZE_BYTES - 1);

    Assertions.assertFalse(VALIDATOR.validate(new S3StorageConfig(null, config)).isEmpty());
  }

  @Test
  public void testPartSizeAboveTheS3MaximumIsRejected()
  {
    final S3TransferConfig config = new S3TransferConfig();
    config.setMinimumUploadPartSize(S3OutputConfig.S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES + 1);

    Assertions.assertFalse(VALIDATOR.validate(new S3StorageConfig(null, config)).isEmpty());
  }

  @Test
  public void testThresholdBelowTheS3MinimumPartSizeIsAccepted()
  {
    final S3TransferConfig config = new S3TransferConfig();
    config.setMultipartUploadThreshold(1024L);

    Assertions.assertTrue(VALIDATOR.validate(new S3StorageConfig(null, config)).isEmpty());
  }

  @Test
  public void testDefaultValues()
  {
    S3TransferConfig config = new S3TransferConfig();
    Assertions.assertTrue(config.isUseTransferManager());
    Assertions.assertEquals(20 * 1024 * 1024L, config.getMinimumUploadPartSize());
    Assertions.assertEquals(20 * 1024 * 1024L, config.getMultipartUploadThreshold());
  }

  @Test
  public void testSetUseTransferManager()
  {
    S3TransferConfig config = new S3TransferConfig();
    config.setUseTransferManager(true);
    Assertions.assertTrue(config.isUseTransferManager());
  }

  @Test
  public void testSetMinimumUploadPartSize()
  {
    S3TransferConfig config = new S3TransferConfig();
    config.setMinimumUploadPartSize(10 * 1024 * 1024L);
    Assertions.assertEquals(10 * 1024 * 1024L, config.getMinimumUploadPartSize());
  }

  @Test
  public void testSetMultipartUploadThreshold()
  {
    S3TransferConfig config = new S3TransferConfig();
    config.setMultipartUploadThreshold(10 * 1024 * 1024L);
    Assertions.assertEquals(10 * 1024 * 1024L, config.getMultipartUploadThreshold());
  }
}
