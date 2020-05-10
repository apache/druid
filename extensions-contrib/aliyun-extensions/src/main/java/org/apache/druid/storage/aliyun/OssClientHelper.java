///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package org.apache.druid.storage.aliyun;
//
//import java.io.ByteArrayInputStream;
//import java.io.File;
//import java.io.InputStream;
//
//import org.apache.druid.java.util.common.StringUtils;
//
//import com.aliyun.oss.OSS;
//import com.aliyun.oss.OSSClientBuilder;
//import com.aliyun.oss.model.AccessControlList;
//import com.aliyun.oss.model.CopyObjectRequest;
//import com.aliyun.oss.model.CopyObjectResult;
//import com.aliyun.oss.model.DeleteObjectsRequest;
//import com.aliyun.oss.model.GenericRequest;
//import com.aliyun.oss.model.GetObjectRequest;
//import com.aliyun.oss.model.ListObjectsRequest;
//import com.aliyun.oss.model.OSSObject;
//import com.aliyun.oss.model.ObjectListing;
//import com.aliyun.oss.model.ObjectMetadata;
//import com.aliyun.oss.model.PutObjectRequest;
//import com.aliyun.oss.model.PutObjectResult;
//
///**
// * {@link AmazonS3} wrapper with {@link ServerSideEncryption}. Every {@link AmazonS3#putObject},
// * {@link AmazonS3#copyObject}, {@link AmazonS3#getObject}, and {@link AmazonS3#getObjectMetadata} methods should be
// * wrapped using ServerSideEncryption.
// *
// * Additional methods can be added to this class if needed, but subclassing AmazonS3Client is discouraged to reduce
// * human mistakes like some methods are not encoded properly.
// */
//public class OssClientHelper
//{
//  private final OSS ossClient;
//
//  public OssClientHelper(OSS ossClient)
//  {
//    this.ossClient = ossClient;
//  }
//
//  public OssClientHelper(String endpoint, String accessKeyId, String secretAccessKey) {
//    this.ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, secretAccessKey);
//  }
//
//public boolean doesObjectExist(String bucket, String objectName)
//  {
//    return ossClient.doesObjectExist(bucket, objectName);
//  }
//
//  public ObjectListing listObjects(ListObjectsRequest request)
//  {
//    return ossClient.listObjects(request);
//  }
//
//  public AccessControlList getBucketAcl(String bucket)
//  {
//    return ossClient.getBucketAcl(bucket);
//  }
//
//  public ObjectMetadata getObjectMetadata(String bucket, String key)
//  {
//    GenericRequest request = new GenericRequest(bucket, key);
//    return ossClient.getobjectm
//  }
//
//  public OSSObject getObject(String bucket, String key)
//  {
//    return getObject(new GetObjectRequest(bucket, key));
//  }
//
//  public OSSObject getObject(GetObjectRequest request)
//  {
//    return ossClient.getObject(request);
//  }
//
//  public PutObjectResult putObject(String bucket, String key, String content)
//  {
//    final InputStream in = new ByteArrayInputStream(StringUtils.toUtf8(content));
//    return putObject(new PutObjectRequest(bucket, key, in, new ObjectMetadata()));
//  }
//
//  public PutObjectResult putObject(String bucket, String key, File file)
//  {
//    return putObject(new PutObjectRequest(bucket, key, file));
//  }
//
//  public PutObjectResult putObject(String bucket, String key, InputStream in, ObjectMetadata objectMetadata)
//  {
//    return putObject(new PutObjectRequest(bucket, key, in, objectMetadata));
//  }
//
//  public PutObjectResult putObject(PutObjectRequest request)
//  {
//    return ossClient.putObject(request);
//  }
//
//  public CopyObjectResult copyObject(CopyObjectRequest request)
//  {
//    return ossClient.copyObject(request);
//  }
//
//  public void deleteObject(String bucket, String key)
//  {
//    ossClient.deleteObject(bucket, key);
//  }
//
//  public void deleteObjects(DeleteObjectsRequest request)
//  {
//    ossClient.deleteObjects(request);
//  }
//}
