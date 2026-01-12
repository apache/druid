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
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AccessControlPolicy;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
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

import java.io.File;

/**
 * {@link S3Client} wrapper with {@link ServerSideEncryption}. Every {@link S3Client#putObject},
 * {@link S3Client#copyObject}, {@link S3Client#getObject}, and {@link S3Client#headObject},
 * {@link S3Client#createMultipartUpload}, @{@link S3Client#uploadPart} methods should be
 * wrapped using ServerSideEncryption.
 * <p>
 * Additional methods can be added to this class if needed, but subclassing AmazonS3Client is discouraged to reduce
 * human mistakes like some methods are not encoded properly.
 */
public class ServerSideEncryptingAmazonS3
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final S3Client amazonS3;
  private final ServerSideEncryption serverSideEncryption;
  private final S3TransferManager transferManager;

  public ServerSideEncryptingAmazonS3(
      S3Client amazonS3,
      ServerSideEncryption serverSideEncryption,
      S3TransferConfig transferConfig
  )
  {
    this.amazonS3 = amazonS3;
    this.serverSideEncryption = serverSideEncryption;
    // Note: S3TransferManager in SDK v2 requires S3AsyncClient, not S3Client.
    // Disabling transfer manager for now - uploads will use the synchronous client.
    this.transferManager = null;
  }

  public S3Client getAmazonS3()
  {
    return amazonS3;
  }

  public boolean doesObjectExist(String bucket, String objectName)
  {
    try {
      // Ignore return value, just want to see if we can get the metadata at all.
      getObjectMetadata(bucket, objectName);
      return true;
    }
    catch (S3Exception e) {
      if (e.awsErrorDetails().sdkHttpResponse().statusCode() == 404) {
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
    return amazonS3.listObjectsV2(request);
  }

  public AccessControlPolicy getBucketAcl(String bucket)
  {
    GetBucketAclResponse response = amazonS3.getBucketAcl(GetBucketAclRequest.builder().bucket(bucket)
                                                                             .build());
    return AccessControlPolicy.builder()
                              .owner(response.owner())
                              .grants(response.grants())
                              .build();
  }

  public HeadObjectResponse getObjectMetadata(String bucket, String key)
  {
    final HeadObjectRequest getObjectMetadataRequest = serverSideEncryption.decorate(
        HeadObjectRequest.builder().bucket(bucket).key(key)
                         .build()
    );
    return amazonS3.headObject(getObjectMetadataRequest);
  }

  public ResponseInputStream<GetObjectResponse> getObject(String bucket, String key)
  {
    return getObject(GetObjectRequest.builder().bucket(bucket).key(key)
                                     .build());
  }

  public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request)
  {
    return amazonS3.getObject(serverSideEncryption.decorate(request));
  }

  public PutObjectResponse putObject(String bucket, String key, File file)
  {
    return putObject(
        PutObjectRequest.builder().bucket(bucket).key(key)
                        .build(), file
    );
  }

  public PutObjectResponse putObject(PutObjectRequest request, File file)
  {
    return amazonS3.putObject(serverSideEncryption.decorate(request), RequestBody.fromFile(file));
  }

  public CopyObjectResponse copyObject(CopyObjectRequest request)
  {
    return amazonS3.copyObject(serverSideEncryption.decorate(request));
  }

  public void deleteObject(String bucket, String key)
  {
    amazonS3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key)
                                             .build());
  }

  public void deleteObjects(DeleteObjectsRequest request)
  {
    amazonS3.deleteObjects(request);
  }


  public CreateMultipartUploadResponse initiateMultipartUpload(CreateMultipartUploadRequest request)
      throws SdkClientException
  {
    return amazonS3.createMultipartUpload(serverSideEncryption.decorate(request));
  }

  public UploadPartResponse uploadPart(UploadPartRequest request, RequestBody requestBody)
      throws SdkClientException
  {
    return amazonS3.uploadPart(serverSideEncryption.decorate(request), requestBody);
  }

  public void cancelMultiPartUpload(AbortMultipartUploadRequest request)
      throws SdkClientException
  {
    amazonS3.abortMultipartUpload(request);
  }

  public CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest request)
      throws SdkClientException
  {
    return amazonS3.completeMultipartUpload(request);
  }

  public void upload(PutObjectRequest request, File file)
  {
    // Note: Transfer manager is disabled due to S3AsyncClient requirement in SDK v2
    // Always use synchronous upload
    putObject(request, file);
  }

  public static class Builder
  {
    private S3ClientBuilder amazonS3ClientBuilder = S3Client.builder();
    private S3StorageConfig s3StorageConfig = new S3StorageConfig(new NoopServerSideEncryption(), null);

    public Builder setAmazonS3ClientBuilder(S3ClientBuilder amazonS3ClientBuilder)
    {
      this.amazonS3ClientBuilder = amazonS3ClientBuilder;
      return this;
    }

    public Builder setS3StorageConfig(S3StorageConfig s3StorageConfig)
    {
      this.s3StorageConfig = s3StorageConfig;
      return this;
    }

    public S3ClientBuilder getAmazonS3ClientBuilder()
    {
      return this.amazonS3ClientBuilder;
    }

    public S3StorageConfig getS3StorageConfig()
    {
      return this.s3StorageConfig;
    }

    public ServerSideEncryptingAmazonS3 build()
    {
      if (amazonS3ClientBuilder == null) {
        throw new ISE("AmazonS3ClientBuilder cannot be null!");
      }
      if (s3StorageConfig == null) {
        throw new ISE("S3StorageConfig cannot be null!");
      }

      S3Client amazonS3Client;
      try {
        amazonS3Client = S3Utils.retryS3Operation(() -> amazonS3ClientBuilder.build());
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      return new ServerSideEncryptingAmazonS3(
          amazonS3Client,
          s3StorageConfig.getServerSideEncryption(),
          s3StorageConfig.getS3TransferConfig()
      );
    }
  }
}
