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

package org.apache.druid.storage.google.output;

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.storage.StorageConnector;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class GoogleExportStorageProviderTest
{
  private final File tempDir = FileUtils.createTempDir();

  private final List<String> validPrefixes = ImmutableList.of(
      "gs://bucket-name/validPath1",
      "gs://bucket-name/validPath2"
  );

  @Test
  public void testGoogleExportStorageProvider()
  {
    GoogleExportStorageProvider googleExportStorageProvider = new GoogleExportStorageProvider("bucket-name", "validPath1");
    googleExportStorageProvider.googleExportConfig = new GoogleExportConfig("tempLocalDir", null, null, validPrefixes);
    StorageConnector storageConnector = googleExportStorageProvider.createStorageConnector(tempDir);
    Assert.assertNotNull(storageConnector);
    Assert.assertTrue(storageConnector instanceof GoogleStorageConnector);

    Assert.assertEquals("gs://bucket-name/validPath1", googleExportStorageProvider.getBasePath());
  }

  @Test
  public void testValidatePaths()
  {
    GoogleExportStorageProvider.validatePrefix(validPrefixes, "bucket-name", "validPath1/");
    GoogleExportStorageProvider.validatePrefix(validPrefixes, "bucket-name", "validPath1");
    GoogleExportStorageProvider.validatePrefix(validPrefixes, "bucket-name", "validPath1/validSubPath/");

    GoogleExportStorageProvider.validatePrefix(ImmutableList.of("gs://bucket-name"), "bucket-name", "");
    GoogleExportStorageProvider.validatePrefix(ImmutableList.of("gs://bucket-name"), "bucket-name", "validPath");
    GoogleExportStorageProvider.validatePrefix(validPrefixes, "bucket-name", "validPath1/../validPath2/");

    Assert.assertThrows(
        DruidException.class,
        () -> GoogleExportStorageProvider.validatePrefix(validPrefixes, "incorrect-bucket", "validPath1/")
    );
    Assert.assertThrows(
        DruidException.class,
        () -> GoogleExportStorageProvider.validatePrefix(validPrefixes, "bucket-name", "invalidPath1")
    );
    Assert.assertThrows(
        DruidException.class,
        () -> GoogleExportStorageProvider.validatePrefix(validPrefixes, "bucket-name", "validPath123")
    );
  }
}
