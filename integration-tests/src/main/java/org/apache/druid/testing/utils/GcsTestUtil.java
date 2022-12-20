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

package org.apache.druid.testing.utils;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

public class GcsTestUtil
{
  private static final Logger LOG = new Logger(AzureTestUtil.class);
  private final Storage googleStorageClient;
  private final String GOOGLE_BUCKET;
  private final String GOOGLE_PREFIX;

  public GcsTestUtil()
  {
    verifyEnvironment();
    GOOGLE_PREFIX = System.getenv("GOOGLE_PREFIX");
    GOOGLE_BUCKET = System.getenv("GOOGLE_BUCKET");
    googleStorageClient = googleStorageClient();
  }

  /**
   * Verify required environment variables are set
   */
  public void verifyEnvironment()
  {
    String[] envVars = {"GOOGLE_BUCKET", "GOOGLE_PREFIX", "GOOGLE_APPLICATION_CREDENTIALS"};
    for (String val : envVars) {
      String envValue = System.getenv(val);
      if (envValue == null) {
        LOG.error("%s was not set", val);
        LOG.error("All of %s MUST be set in the environment", Arrays.toString(envVars));
      }
    }
  }

  /**
   * Gets the Google cloud storage client using credentials set in GOOGLE_APPLICATION_CREDENTIALS
   */
  private Storage googleStorageClient()
  {
    return StorageOptions.getDefaultInstance().getService();
  }

  public void uploadFileToGcs(String filePath) throws IOException
  {
    File source = new File(filePath);
    BlobId blobId = BlobId.of(GOOGLE_BUCKET, source.getName());
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    googleStorageClient.create(blobInfo, Files.readAllBytes(source.toPath()));
  }

  public void deleteFileFromGcs(String gcsObjectName) throws IOException
  {
    Blob blob = googleStorageClient.get(GOOGLE_BUCKET, gcsObjectName);
    if (blob == null) {
      LOG.warn("The object " + gcsObjectName + " wasn't found in " + GOOGLE_BUCKET);
      return;
    }
    googleStorageClient.delete(GOOGLE_BUCKET, gcsObjectName);
  }

  public void deletePrefixFolderFromGcs()
  {
    Iterable<Blob> blobs = googleStorageClient.list(GOOGLE_BUCKET, Storage.BlobListOption.prefix(GOOGLE_PREFIX)).iterateAll();
    for (Blob blob : blobs) {
      blob.delete(Blob.BlobSourceOption.generationMatch());
    }
  }
}
