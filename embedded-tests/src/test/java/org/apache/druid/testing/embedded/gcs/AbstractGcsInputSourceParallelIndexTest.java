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

package org.apache.druid.testing.embedded.gcs;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.google.output.GoogleStorageConnectorModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.indexer.AbstractCloudInputSourceParallelIndexTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

public class AbstractGcsInputSourceParallelIndexTest extends AbstractCloudInputSourceParallelIndexTest
{
  private static final Logger LOG = new Logger(AbstractGcsInputSourceParallelIndexTest.class);
  private GcsTestUtil gcs;
  private final GoogleCloudStorageResource gcsResource = new GoogleCloudStorageResource();

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    cluster.addExtension(GoogleStorageConnectorModule.class)
           .addResource(gcsResource);
  }

  @BeforeAll
  public void uploadDataFilesToGcs()
  {
    LOG.info("Uploading data files to GCS");
    String localPath = "data/json/";
    try {
      gcs = new GcsTestUtil(
          gcsResource.getUrl(),
          getCloudBucket("google"),
          getCloudPath("google")
      );
      for (String file : fileList()) {
        gcs.uploadFileToGcs(localPath + file, "application/json");
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterEach
  public void deleteSegmentsFromGcs()
  {
    // Deleting folder created for storing segments (by druid) after test is completed
    try {
      gcs.deletePrefixFolderFromGcs(dataSource);
    }
    catch (Exception e) {
      LOG.warn(e, "Unable to delete segments from GCS");
    }
  }

  @AfterAll
  public void deleteDataFilesFromGcs()
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
