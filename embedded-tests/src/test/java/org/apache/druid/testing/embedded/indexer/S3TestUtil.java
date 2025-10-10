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

package org.apache.druid.testing.embedded.indexer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.indexing.Resources;

import java.util.ArrayList;
import java.util.List;

public class S3TestUtil
{
  private static final Logger LOG = new Logger(S3TestUtil.class);

  private final AmazonS3 s3Client;
  private final String path;
  private final String bucket;

  public S3TestUtil(AmazonS3 s3Client, String bucket, String path)
  {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.path = path;
  }

  public void createBucket()
  {
    s3Client.createBucket(bucket);
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
      String s3ObjectPath = path + "/" + file.substring(file.lastIndexOf('/') + 1);
      s3ObjectPaths.add(s3ObjectPath);
      try {
        s3Client.putObject(
            bucket,
            s3ObjectPath,
            Resources.getFileForResource(file)
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
        keys.add(new KeyVersion(path + "/" + fileName));
      }
      DeleteObjectsRequest delObjReq = new DeleteObjectsRequest(bucket)
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
          .withBucketName(bucket)
          .withPrefix(path + "/" + datasource + "/");

      ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);

      while (true) {
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          s3Client.deleteObject(bucket, objectSummary.getKey());
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
