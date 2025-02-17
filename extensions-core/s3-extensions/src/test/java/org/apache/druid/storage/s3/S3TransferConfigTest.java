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

import org.junit.Assert;
import org.junit.Test;

public class S3TransferConfigTest
{
  @Test
  public void testDefaultValues()
  {
    S3TransferConfig config = new S3TransferConfig();
    Assert.assertFalse(config.isUseTransferManager());
    Assert.assertEquals(5 * 1024 * 1024L, config.getMinimumUploadPartSize());
    Assert.assertEquals(5 * 1024 * 1024L, config.getMultipartUploadThreshold());
  }

  @Test
  public void testSetUseTransferManager()
  {
    S3TransferConfig config = new S3TransferConfig();
    config.setUseTransferManager(true);
    Assert.assertTrue(config.isUseTransferManager());
  }

  @Test
  public void testSetMinimumUploadPartSize()
  {
    S3TransferConfig config = new S3TransferConfig();
    config.setMinimumUploadPartSize(10 * 1024 * 1024L);
    Assert.assertEquals(10 * 1024 * 1024L, config.getMinimumUploadPartSize());
  }

  @Test
  public void testSetMultipartUploadThreshold()
  {
    S3TransferConfig config = new S3TransferConfig();
    config.setMultipartUploadThreshold(10 * 1024 * 1024L);
    Assert.assertEquals(10 * 1024 * 1024L, config.getMultipartUploadThreshold());
  }
}
