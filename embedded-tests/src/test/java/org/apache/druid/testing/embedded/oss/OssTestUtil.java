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

package org.apache.druid.testing.embedded.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.indexing.Resources;

import java.util.ArrayList;
import java.util.List;

public class OssTestUtil
{
  private static final Logger LOG = new Logger(OssTestUtil.class);

  private final OSS ossClient;
  private final String bucket;
  private final String path;

  public OssTestUtil(OSS ossClient, String bucket, String path)
  {
    this.ossClient = ossClient;
    this.bucket = bucket;
    this.path = path;
  }

  /**
   * Uploads a list of local resource-relative files to OSS at the configured bucket and path prefix.
   *
   * @param localFiles resource-relative paths to upload (e.g. {@code "data/json/tiny_wiki_1.json"})
   */
  public void uploadDataFilesToOss(List<String> localFiles)
  {
    List<String> ossObjectPaths = new ArrayList<>();
    for (String file : localFiles) {
      final String ossKey = path + "/" + file.substring(file.lastIndexOf('/') + 1);
      ossObjectPaths.add(ossKey);
      try {
        ossClient.putObject(bucket, ossKey, Resources.getFileForResource(file));
        LOG.info("Uploaded [%s] to oss://%s/%s", file, bucket, ossKey);
      }
      catch (Exception e) {
        LOG.error(e, "Unable to upload file [%s] to OSS", file);
        deleteFilesFromOss(ossObjectPaths);
        throw e;
      }
    }
  }

  /**
   * Deletes the given filenames from the configured bucket and path prefix.
   *
   * @param fileNames bare file names (without path) to delete
   */
  public void deleteFilesFromOss(List<String> fileNames)
  {
    try {
      final List<String> keys = new ArrayList<>();
      for (String fileName : fileNames) {
        keys.add(path + "/" + fileName);
      }
      final DeleteObjectsRequest request = new DeleteObjectsRequest(bucket);
      request.setKeys(keys);
      ossClient.deleteObjects(request);
    }
    catch (Exception e) {
      LOG.warn(e, "Unable to delete data files from OSS");
    }
  }

  /**
   * Deletes all objects under the configured path prefix that belong to the given datasource folder.
   *
   * @param datasource folder to delete from OSS (typically the test datasource name)
   */
  public void deleteFolderFromOss(String datasource)
  {
    try {
      final ListObjectsRequest listRequest = new ListObjectsRequest(bucket);
      listRequest.setPrefix(path + "/" + datasource + "/");
      ObjectListing listing = ossClient.listObjects(listRequest);
      while (true) {
        final List<String> keys = new ArrayList<>();
        for (OSSObjectSummary summary : listing.getObjectSummaries()) {
          keys.add(summary.getKey());
        }
        if (!keys.isEmpty()) {
          final DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket);
          deleteRequest.setKeys(keys);
          ossClient.deleteObjects(deleteRequest);
        }
        if (listing.isTruncated()) {
          listRequest.setMarker(listing.getNextMarker());
          listing = ossClient.listObjects(listRequest);
        } else {
          break;
        }
      }
    }
    catch (Exception e) {
      LOG.warn(e, "Unable to delete folder from OSS");
    }
  }
}
