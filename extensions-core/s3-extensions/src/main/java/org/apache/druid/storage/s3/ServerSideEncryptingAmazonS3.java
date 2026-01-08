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

import org.apache.druid.java.util.common.ISE;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import javax.annotation.Nullable;
import java.io.File;
import java.io.InputStream;
import java.util.function.Supplier;

/**
 * {@link S3Client} wrapper with {@link ServerSideEncryption}. Every putObject,
 * copyObject, getObject, and headObject, createMultipartUpload, uploadPart methods
 * should be wrapped using ServerSideEncryption.
 * <p>
 * AWS SDK v2 uses immutable request objects with builders, so the decorator
 * pattern works with builders rather than modifying requests in place.
 */
public class ServerSideEncryptingAmazonS3
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final S3Client s3Client;
  private final ServerSideEncryption serverSideEncryption;
  @Nullable
  private final S3TransferManager transferManager;

  public ServerSideEncryptingAmazonS3(
      S3Client s3Client,
      ServerSideEncryption serverSideEncryption,
      S3TransferConfig transferConfig
  )
  {
    this.s3Client = s3Client;
    this.serverSideEncryption = serverSideEncryption;
    // Note: S3TransferManager in SDK v2 requires an async client.
    // For simplicity, we disable transfer manager and use sync uploads.
    // The transfer manager can be enabled later if async operations are needed.
    this.transferManager = null;
  }

  public S3Client getS3Client()
  {
    return s3Client;
  }

  public boolean doesObjectExist(String bucket, String objectName)
  {
    try {
      // Ignore return value, just want to see if we can get the metadata at all.
      getObjectMetadata(bucket, objectName);
      return true;
    }
    catch (S3Exception e) {
      if (e.statusCode() == 404) {
        // Object not found.
        return false;
      } else {
        // Some other error: re-throw.
        throw e;
      }
    }
  }

  public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request)
  {
    return s3Client.listObjectsV2(request);
  }

  public GetBucketAclResponse getBucketAcl(String bucket)
  {
    return s3Client.getBucketAcl(builder -> builder.bucket(bucket));
  }

  public HeadObjectResponse getObjectMetadata(String bucket, String key)
  {
    HeadObjectRequest.Builder requestBuilder = HeadObjectRequest.builder()
        .bucket(bucket)
        .key(key);
    HeadObjectRequest request = serverSideEncryption.decorate(requestBuilder).build();
    return s3Client.headObject(request);
  }

  public ResponseInputStream<GetObjectResponse> getObject(String bucket, String key)
  {
    GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
        .bucket(bucket)
        .key(key);
    return getObject(requestBuilder);
  }

  public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest.Builder requestBuilder)
  {
    GetObjectRequest request = serverSideEncryption.decorate(requestBuilder).build();
    return s3Client.getObject(request);
  }

  public PutObjectResponse putObject(String bucket, String key, File file)
  {
    PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
        .bucket(bucket)
        .key(key);
    return putObject(requestBuilder, RequestBody.fromFile(file));
  }

  public PutObjectResponse putObject(PutObjectRequest.Builder requestBuilder, RequestBody requestBody)
  {
    PutObjectRequest request = serverSideEncryption.decorate(requestBuilder).build();
    return s3Client.putObject(request, requestBody);
  }

  public PutObjectResponse putObject(String bucket, String key, InputStream inputStream, long contentLength)
  {
    PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .contentLength(contentLength);
    return putObject(requestBuilder, RequestBody.fromInputStream(inputStream, contentLength));
  }

  public CopyObjectResponse copyObject(CopyObjectRequest.Builder requestBuilder)
  {
    CopyObjectRequest request = serverSideEncryption.decorate(requestBuilder).build();
    return s3Client.copyObject(request);
  }

  public void deleteObject(String bucket, String key)
  {
    DeleteObjectRequest request = DeleteObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build();
    s3Client.deleteObject(request);
  }

  public DeleteObjectsResponse deleteObjects(DeleteObjectsRequest request)
  {
    return s3Client.deleteObjects(request);
  }

  public CreateMultipartUploadResponse createMultipartUpload(CreateMultipartUploadRequest.Builder requestBuilder)
      throws SdkClientException
  {
    CreateMultipartUploadRequest request = serverSideEncryption.decorate(requestBuilder).build();
    return s3Client.createMultipartUpload(request);
  }

  public UploadPartResponse uploadPart(UploadPartRequest.Builder requestBuilder, RequestBody requestBody)
      throws SdkClientException
  {
    UploadPartRequest request = serverSideEncryption.decorate(requestBuilder).build();
    return s3Client.uploadPart(request, requestBody);
  }

  public void abortMultipartUpload(AbortMultipartUploadRequest request)
      throws SdkClientException
  {
    s3Client.abortMultipartUpload(request);
  }

  public CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest request)
      throws SdkClientException
  {
    return s3Client.completeMultipartUpload(request);
  }

  /**
   * Upload a file to S3, optionally using the transfer manager for large files.
   *
   * @param bucket    the S3 bucket
   * @param key       the S3 key
   * @param file      the file to upload
   * @param aclGrant  optional ACL grant to apply (may be null)
   */
  public void upload(String bucket, String key, File file, @Nullable Grant aclGrant)
  {
    if (transferManager == null) {
      PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
          .bucket(bucket)
          .key(key);
      if (aclGrant != null) {
        requestBuilder.grantFullControl(aclGrant.grantee().id());
      }
      putObject(requestBuilder, RequestBody.fromFile(file));
    } else {
      PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
          .bucket(bucket)
          .key(key);
      if (aclGrant != null) {
        requestBuilder.grantFullControl(aclGrant.grantee().id());
      }
      PutObjectRequest decoratedRequest = serverSideEncryption.decorate(requestBuilder).build();

      UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
          .putObjectRequest(decoratedRequest)
          .source(file)
          .build();

      FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);
      fileUpload.completionFuture().join();
    }
  }

  public static class Builder
  {
    private Supplier<S3Client> s3ClientSupplier;
    private S3StorageConfig s3StorageConfig = new S3StorageConfig(new NoopServerSideEncryption(), null);

    public Builder setS3ClientSupplier(Supplier<S3Client> s3ClientSupplier)
    {
      this.s3ClientSupplier = s3ClientSupplier;
      return this;
    }

    public Builder setS3StorageConfig(S3StorageConfig s3StorageConfig)
    {
      this.s3StorageConfig = s3StorageConfig;
      return this;
    }

    public S3StorageConfig getS3StorageConfig()
    {
      return this.s3StorageConfig;
    }

    public ServerSideEncryptingAmazonS3 build()
    {
      if (s3ClientSupplier == null) {
        throw new ISE("S3Client supplier cannot be null!");
      }
      if (s3StorageConfig == null) {
        throw new ISE("S3StorageConfig cannot be null!");
      }

      S3Client s3Client;
      try {
        s3Client = S3Utils.retryS3Operation(s3ClientSupplier::get);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      return new ServerSideEncryptingAmazonS3(
          s3Client,
          s3StorageConfig.getServerSideEncryption(),
          s3StorageConfig.getS3TransferConfig()
      );
    }
  }
}
