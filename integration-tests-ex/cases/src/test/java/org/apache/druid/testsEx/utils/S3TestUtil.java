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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.utils.TestQueryHelper;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class S3TestUtil
{
  public static final Logger LOG = new Logger(TestQueryHelper.class);

  private final AmazonS3 s3Client;
  private final String S3_ACCESS_KEY;
  private final String S3_SECRET_KEY;
  private final String S3_REGION;
  private final String S3_CLOUD_PATH;
  private final String S3_CLOUD_BUCKET;

  public S3TestUtil()
  {
    verifyEnvironment();
    S3_ACCESS_KEY = System.getenv("AWS_ACCESS_KEY_ID");
    S3_SECRET_KEY = System.getenv("AWS_SECRET_ACCESS_KEY");
    S3_REGION = System.getenv("AWS_REGION");
    S3_CLOUD_PATH = System.getenv("DRUID_CLOUD_PATH");
    S3_CLOUD_BUCKET = System.getenv("DRUID_CLOUD_BUCKET");
    s3Client = s3Client();
  }

  /**
   * Verify required environment variables are set for
   */
  public void verifyEnvironment()
  {
    String[] envVars = {"DRUID_CLOUD_BUCKET", "DRUID_CLOUD_PATH", "AWS_ACCESS_KEY_ID",
                        "AWS_SECRET_ACCESS_KEY", "AWS_REGION"};
    for (String val : envVars) {
      String envValue = System.getenv(val);
      if (envValue == null) {
        LOG.error("%s was not set", val);
        LOG.error("All of %s MUST be set in the environment", Arrays.toString(envVars));
      }
    }
  }

  /**
   * Creates a s3Client which will be used for uploading and deleting files from s3
   */
  private AmazonS3 s3Client()
  {
    AWSCredentials credentials = new BasicAWSCredentials(S3_ACCESS_KEY, S3_SECRET_KEY);
    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.setProtocol(Protocol.HTTP);
    return AmazonS3ClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        // Setting endpoint to MinIO S3 API endpoint (e.g., "http://localhost:9000")
        // configured in integration-tests-ex/cases/cluster/Common/dependencies.yaml
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:9000", S3_REGION))
        .withPathStyleAccessEnabled(true)
        .withClientConfiguration(clientConfig)
        .build();
  }

  /**
   * Uploads a list of files to s3 at the location set in the IT config
   *
   * @param  localFiles List of local path of files
   */
  public void uploadDataFilesToS3(List<String> localFiles)
  {
    List<String> s3ObjectPaths = new ArrayList<>();
    for (String file : localFiles) {
      String s3ObjectPath = S3_CLOUD_PATH + "/" + file.substring(file.lastIndexOf('/') + 1);
      s3ObjectPaths.add(s3ObjectPath);
      try {
        s3Client.putObject(
            S3_CLOUD_BUCKET,
            s3ObjectPath,
            new File(file)
        );
      }
      catch (Exception e) {
        LOG.error("Unable to upload file %s", file);
        // Delete rest of the uploaded files
        deleteFilesFromS3(s3ObjectPaths);
        // Pass the exception forward for the test to handle
        throw e;
      }
    }
  }

  /**
   * Deletes a list of files to s3 at the location set in the IT config
   *
   * @param  fileList List of path of files inside a s3 bucket
   */
  public void deleteFilesFromS3(List<String> fileList)
  {
    try {
      ArrayList<KeyVersion> keys = new ArrayList<>();
      for (String fileName : fileList) {
        keys.add(new KeyVersion(S3_CLOUD_PATH + "/" + fileName));
      }
      DeleteObjectsRequest delObjReq = new DeleteObjectsRequest(S3_CLOUD_BUCKET)
          .withKeys(keys);
      s3Client.deleteObjects(delObjReq);
    }
    catch (Exception e) {
      // Posting warn instead of error as not being able to delete files from s3 does not impact the test.
      LOG.warn(e, "Unable to delete data files from s3");
    }
  }

  /**
   * Deletes all files in a folder in s3 bucket
   *
   * @param  datasource Path of folder inside a s3 bucket
   */
  public void deleteFolderFromS3(String datasource)
  {
    try {
      // Delete segments created by druid
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(S3_CLOUD_BUCKET)
          .withPrefix(S3_CLOUD_PATH + "/" + datasource + "/");

      ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);

      while (true) {
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          s3Client.deleteObject(S3_CLOUD_BUCKET, objectSummary.getKey());
        }
        if (objectListing.isTruncated()) {
          objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } else {
          break;
        }
      }
    }
    catch (Exception e) {
      // Posting warn instead of error as not being able to delete files from s3 does not impact the test.
      LOG.warn(e, "Unable to delete folder from s3");
    }
  }
}
