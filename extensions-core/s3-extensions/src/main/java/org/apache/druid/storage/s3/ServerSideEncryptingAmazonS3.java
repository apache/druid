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

package org.apache.druid.storage.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.druid.java.util.common.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;

/**
 * {@link AmazonS3} wrapper with {@link ServerSideEncryption}. Every {@link AmazonS3#putObject},
 * {@link AmazonS3#copyObject}, {@link AmazonS3#getObject}, and {@link AmazonS3#getObjectMetadata} methods should be
 * wrapped using ServerSideEncryption.
 *
 * Additional methods can be added to this class if needed, but subclassing AmazonS3Client is discouraged to reduce
 * human mistakes like some methods are not encoded properly.
 */
public class ServerSideEncryptingAmazonS3
{
  private final AmazonS3 amazonS3;
  private final ServerSideEncryption serverSideEncryption;

  public ServerSideEncryptingAmazonS3(AmazonS3 amazonS3, ServerSideEncryption serverSideEncryption)
  {
    this.amazonS3 = amazonS3;
    this.serverSideEncryption = serverSideEncryption;
  }

  public boolean doesObjectExist(String bucket, String objectName)
  {
    return amazonS3.doesObjectExist(bucket, objectName);
  }

  public ListObjectsV2Result listObjectsV2(ListObjectsV2Request request)
  {
    return amazonS3.listObjectsV2(request);
  }

  public AccessControlList getBucketAcl(String bucket)
  {
    return amazonS3.getBucketAcl(bucket);
  }

  public ObjectMetadata getObjectMetadata(String bucket, String key)
  {
    final GetObjectMetadataRequest getObjectMetadataRequest = serverSideEncryption.decorate(
        new GetObjectMetadataRequest(bucket, key)
    );
    return amazonS3.getObjectMetadata(getObjectMetadataRequest);
  }

  public S3Object getObject(String bucket, String key)
  {
    return getObject(new GetObjectRequest(bucket, key));
  }

  public S3Object getObject(GetObjectRequest request)
  {
    return amazonS3.getObject(serverSideEncryption.decorate(request));
  }

  public PutObjectResult putObject(String bucket, String key, String content)
  {
    final InputStream in = new ByteArrayInputStream(StringUtils.toUtf8(content));
    return putObject(new PutObjectRequest(bucket, key, in, new ObjectMetadata()));
  }

  public PutObjectResult putObject(String bucket, String key, File file)
  {
    return putObject(new PutObjectRequest(bucket, key, file));
  }

  public PutObjectResult putObject(String bucket, String key, InputStream in, ObjectMetadata objectMetadata)
  {
    return putObject(new PutObjectRequest(bucket, key, in, objectMetadata));
  }

  public PutObjectResult putObject(PutObjectRequest request)
  {
    return amazonS3.putObject(serverSideEncryption.decorate(request));
  }

  public CopyObjectResult copyObject(CopyObjectRequest request)
  {
    return amazonS3.copyObject(serverSideEncryption.decorate(request));
  }

  public void deleteObject(String bucket, String key)
  {
    amazonS3.deleteObject(bucket, key);
  }
}
