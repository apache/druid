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

import com.google.api.client.http.FileContent;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.common.base.Predicates;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleUtils;
import org.apache.druid.testing.embedded.indexing.Resources;

import java.io.File;
import java.io.IOException;

public class GcsTestUtil
{
  private static final Logger LOG = new Logger(GcsTestUtil.class);
  private final GoogleStorage googleStorageClient;
  private final String bucket;
  private final String pathPrefix;

  public GcsTestUtil(String storageUrl, String bucket, String pathPrefix)
  {
    this.bucket = bucket;
    this.pathPrefix = pathPrefix;

    try (Storage storage = GoogleStorageTestModule.createStorageForTests(storageUrl)) {
      storage.create(BucketInfo.of(bucket));
      this.googleStorageClient = new GoogleStorage(() -> storage);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void uploadFileToGcs(String filePath, String contentType) throws IOException
  {
    LOG.info("Uploading file %s at path %s in bucket %s", filePath, pathPrefix, bucket);
    File file = Resources.getFileForResource(filePath);
    googleStorageClient.insert(
        bucket,
        pathPrefix + "/" + file.getName(),
        new FileContent(contentType, file),
        null
    );
  }

  public void deleteFileFromGcs(String gcsObjectName)
  {
    LOG.info("Deleting object %s at path %s in bucket %s", gcsObjectName, pathPrefix, bucket);
    googleStorageClient.delete(bucket, pathPrefix + "/" + gcsObjectName);
  }

  public void deletePrefixFolderFromGcs(String datasource) throws Exception
  {
    LOG.info("Deleting path %s in bucket %s", pathPrefix, bucket);
    GoogleUtils.deleteObjectsInPath(
        googleStorageClient,
        new GoogleInputDataConfig(),
        bucket,
        pathPrefix + "/" + datasource,
        Predicates.alwaysTrue()
    );
  }
}
