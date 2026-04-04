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

package org.apache.druid.testing.embedded.minio;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.indexing.Resources;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class S3TestUtil
{
  private static final Logger LOG = new Logger(S3TestUtil.class);

  private final S3Client s3Client;
  private final String path;
  private final String bucket;

  public S3TestUtil(S3Client s3Client, String bucket, String path)
  {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.path = path;
  }

  public void createBucket()
  {
    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
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
        File localFile = Resources.getFileForResource(file);
        s3Client.putObject(
            PutObjectRequest.builder()
                .bucket(bucket)
                .key(s3ObjectPath)
                .build(),
            RequestBody.fromFile(localFile)
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
      List<ObjectIdentifier> keys = fileList.stream()
          .map(fileName -> ObjectIdentifier.builder().key(path + "/" + fileName).build())
          .collect(Collectors.toList());

      s3Client.deleteObjects(DeleteObjectsRequest.builder()
          .bucket(bucket)
          .delete(Delete.builder().objects(keys).build())
          .build());
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
      String prefix = path + "/" + datasource + "/";
      String continuationToken = null;

      do {
        ListObjectsRequest.Builder requestBuilder = ListObjectsRequest.builder()
            .bucket(bucket)
            .prefix(prefix);

        if (continuationToken != null) {
          requestBuilder.marker(continuationToken);
        }

        ListObjectsResponse response = s3Client.listObjects(requestBuilder.build());

        for (S3Object objectSummary : response.contents()) {
          s3Client.deleteObject(DeleteObjectRequest.builder()
              .bucket(bucket)
              .key(objectSummary.key())
              .build());
        }

        if (response.isTruncated()) {
          List<S3Object> contents = response.contents();
          continuationToken = contents.isEmpty() ? null : contents.get(contents.size() - 1).key();
        } else {
          continuationToken = null;
        }
      } while (continuationToken != null);
    }
    catch (Exception e) {
      // Posting warn instead of error as not being able to delete files from s3 does not impact the test.
      LOG.warn(e, "Unable to delete folder from s3");
    }
  }
}
