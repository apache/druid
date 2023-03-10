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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testsEx.utils.GcsTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

/**
 * This class defines methods to upload and delete the data files used by the tests, which will inherit this class.
 * The files are uploaded based on the values set for following environment variables.
 * "GOOGLE_BUCKET", "GOOGLE_PREFIX", "GOOGLE_APPLICATION_CREDENTIALS"
 * The test will fail if the above variables are not set.
 */
public class AbstractGcsInputSourceParallelIndexTest extends AbstractCloudInputSourceParallelIndexTest
{
  private static final Logger LOG = new Logger(AbstractGcsInputSourceParallelIndexTest.class);
  private static GcsTestUtil gcs;

  @BeforeClass
  public static void uploadDataFilesToGcs()
  {
    LOG.info("Uploading data files to GCS");
    String localPath = "resources/data/batch_index/json/";
    try {
      gcs = new GcsTestUtil();
      for (String file : fileList()) {
        gcs.uploadFileToGcs(localPath + file, "application/json");
      }
    }
    catch (Exception e) {
      LOG.error(e, "Unable to upload files to GCS");
      // Fail if exception
      Assert.fail(e.getMessage());
    }
  }

  @After
  public void deleteSegmentsFromGcs()
  {
    // Deleting folder created for storing segments (by druid) after test is completed
    try {
      gcs.deletePrefixFolderFromGcs(indexDatasource);
    }
    catch (Exception e) {
      LOG.warn(e, "Unable to delete segments from GCS");
    }
  }

  @AfterClass
  public static void deleteDataFilesFromGcs()
  {
    LOG.info("Deleting data files from GCS");
    try {
      for (String file : fileList()) {
        // Deleting uploaded data files
        gcs.deleteFileFromGcs(file);
      }
    }
    catch (Exception e) {
      LOG.warn(e, "Unable to delete files in GCS");
    }
  }
}
