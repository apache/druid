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
import org.apache.druid.testsEx.utils.S3TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;

/**
 * This class defines methods to upload and delete the data files used by the tests, which will inherit this class.
 * The files are uploaded based on the values set for following environment variables.
 * "DRUID_CLOUD_BUCKET", "DRUID_CLOUD_PATH", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"
 * The test will fail if the above variables are not set.
 */
public abstract class AbstractS3InputSourceParallelIndexTest extends AbstractCloudInputSourceParallelIndexTest
{
  private static final Logger LOG = new Logger(AbstractS3InputSourceParallelIndexTest.class);
  private static S3TestUtil s3;

  @BeforeClass
  public static void uploadDataFilesToS3()
  {
    List<String> filesToUpload = new ArrayList<>();
    String localPath = "resources/data/batch_index/json/";
    for (String file : fileList()) {
      filesToUpload.add(localPath + file);
    }
    try {
      s3 = new S3TestUtil();
      s3.uploadDataFilesToS3(filesToUpload);
    }
    catch (Exception e) {
      LOG.error(e, "Unable to upload files to s3");
      // Fail if exception
      Assert.fail(e.getMessage());
    }
  }

  @After
  public void deleteSegmentsFromS3()
  {
    // Deleting folder created for storing segments (by druid) after test is completed
    s3.deleteFolderFromS3(indexDatasource);
  }

  @AfterClass
  public static void deleteDataFilesFromS3()
  {
    // Deleting uploaded data files
    s3.deleteFilesFromS3(fileList());
  }
}
