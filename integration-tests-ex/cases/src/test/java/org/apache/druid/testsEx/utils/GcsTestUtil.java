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

package org.apache.druid.testsEx.utils;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Predicates;
import com.google.common.base.Suppliers;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleUtils;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;

public class GcsTestUtil
{
  private static final Logger LOG = new Logger(AzureTestUtil.class);
  private final GoogleStorage googleStorageClient;
  private final String GOOGLE_BUCKET;
  private final String GOOGLE_PREFIX;

  public GcsTestUtil() throws GeneralSecurityException, IOException
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
  private GoogleStorage googleStorageClient() throws GeneralSecurityException, IOException
  {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    GoogleCredential credential = GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(Collections.singleton("https://www.googleapis.com/auth/cloud-platform"));
    }
    LOG.info("Creating Storage object");
    GoogleCredential finalCredential = credential;
    return new GoogleStorage(
        Suppliers.memoize(
            () -> StorageOptions.getDefaultInstance().getService()
        )
    );
  }

  public void uploadFileToGcs(String filePath, String contentType) throws IOException
  {
    LOG.info("Uploading file %s at path %s in bucket %s", filePath, GOOGLE_PREFIX, GOOGLE_BUCKET);
    File file = new File(filePath);
    googleStorageClient.insert(GOOGLE_BUCKET,
                               GOOGLE_PREFIX + "/" + file.getName(),
                               new FileContent(contentType, file)
    );
  }

  public void deleteFileFromGcs(String gcsObjectName) throws IOException
  {
    LOG.info("Deleting object %s at path %s in bucket %s", gcsObjectName, GOOGLE_PREFIX, GOOGLE_BUCKET);
    googleStorageClient.delete(GOOGLE_BUCKET, GOOGLE_PREFIX + "/" + gcsObjectName);
  }

  public void deletePrefixFolderFromGcs(String datasource) throws Exception
  {
    LOG.info("Deleting path %s in bucket %s", GOOGLE_PREFIX, GOOGLE_BUCKET);
    GoogleUtils.deleteObjectsInPath(
        googleStorageClient,
        new GoogleInputDataConfig(),
        GOOGLE_BUCKET,
        GOOGLE_PREFIX + "/" + datasource,
        Predicates.alwaysTrue()
    );
  }
}
