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

package org.apache.druid.testsEx.indexer;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testsEx.utils.AzureTestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.net.URI;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * This class defines methods to upload and delete the data files used by the tests, which will inherit this class.
 * The files are uploaded based on the values set for following environment variables.
 * "AZURE_KEY", "AZURE_ACCOUNT", "AZURE_CONTAINER", "DRUID_CLOUD_PATH"
 * The test will fail if the above variables are not set.
 */
public class AbstractAzureInputSourceParallelIndexTest extends AbstractCloudInputSourceParallelIndexTest
{
  private static final Logger LOG = new Logger(AbstractAzureInputSourceParallelIndexTest.class);

  static AzureTestUtil azure;

  @BeforeClass
  public static void uploadDataFilesToAzure()
  {
    try {
      LOG.info("Uploading files to Azure");
      azure = new AzureTestUtil();
      // Creating a container with name set in AZURE_CONTAINER env variable.
      azure.createStorageContainer();
      String localPath = "resources/data/batch_index/json/";
      for (String file : fileList()) {
        azure.uploadFileToContainer(localPath + file);
      }
    }
    catch (Exception e) {
      LOG.error(e, "Unable to upload files to azure");
      // Fail if exception
      fail();
    }
  }

  @AfterClass
  public static void deleteDataFilesFromAzure()
  {
    try {
      // Deleting uploaded data files
      azure.deleteStorageContainer();
    }
    catch (Exception e) {
      LOG.warn(e, "Unable to delete container in azure");
    }
  }

  public static void validateAzureSegmentFilesDeleted(String path)
  {
    List<URI> segmentFiles = ImmutableList.of();
    try {
      segmentFiles = azure.listFiles(path);
    }
    catch (Exception e) {
      LOG.warn(e, "Failed to validate that azure segment files were deleted.");
    }
    finally {
      Assert.assertEquals(
            "Some segment files were not deleted: " + segmentFiles,
            segmentFiles.size(),
            0
      );
    }
  }
}
