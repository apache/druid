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

import org.apache.druid.common.aws.AWSModule;
import org.apache.druid.data.input.s3.S3InputSourceConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.containers.MinIOContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.net.URI;

/**
 * A MinIO container resource for use in embedded tests as deep storage.
 * Sets up MinIO as S3-compatible storage and configures Druid's S3 connector.
 */
public class MinIOStorageResource extends TestcontainerResource<MinIOContainer>
{
  private static final String MINIO_IMAGE = "minio/minio:latest";
  private static final String DEFAULT_BUCKET = "druid-deep-storage";
  private static final String DEFAULT_BASE_KEY = "druid/segments";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";

  private final String bucket;
  private final String baseKey;
  private S3Client s3Client;

  public MinIOStorageResource()
  {
    this(DEFAULT_BUCKET, DEFAULT_BASE_KEY);
  }

  public MinIOStorageResource(String bucket, String baseKey)
  {
    this.bucket = bucket;
    this.baseKey = baseKey;
  }

  @Override
  protected MinIOContainer createContainer()
  {
    return new MinIOContainer(MINIO_IMAGE)
        .withUserName(getAccessKey())
        .withPassword(getSecretKey());
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    s3Client = createS3Client();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

    cluster.addExtension(S3StorageDruidModule.class);
    cluster.addExtension(AWSModule.class);

    // Configure storage bucket and base key
    cluster.addCommonProperty("druid.storage.type", "s3");
    cluster.addCommonProperty("druid.storage.bucket", getBucket());
    cluster.addCommonProperty("druid.storage.baseKey", getBaseKey());

    // Configure indexer logs
    cluster.addCommonProperty("druid.indexer.logs.type", "s3");
    cluster.addCommonProperty("druid.indexer.logs.s3Bucket", getBucket());
    cluster.addCommonProperty("druid.indexer.logs.s3Prefix", "druid/indexing-logs");

    // Configure S3 connection properties
    cluster.addCommonProperty("druid.s3.endpoint.url", cluster.getEmbeddedHostname().useInUri(getEndpointUrl()));
    // AWS SDK v2 requires a region; use a fixed value since MinIO doesn't validate it
    cluster.addCommonProperty("druid.s3.endpoint.signingRegion", "us-east-1");
    cluster.addCommonProperty("druid.s3.accessKey", getAccessKey());
    cluster.addCommonProperty("druid.s3.secretKey", getSecretKey());
    cluster.addCommonProperty("druid.s3.enablePathStyleAccess", "true");
    cluster.addCommonProperty("druid.s3.protocol", "http");
  }

  public String getBucket()
  {
    return bucket;
  }

  public String getBaseKey()
  {
    return baseKey;
  }

  public String getAccessKey()
  {
    return ACCESS_KEY;
  }

  public String getSecretKey()
  {
    return SECRET_KEY;
  }

  public String getEndpointUrl()
  {
    ensureRunning();
    return getContainer().getS3URL();
  }

  public S3Client getS3Client()
  {
    ensureRunning();
    return s3Client;
  }

  /**
   * Creates temporary S3 credentials using the AssumeRole STS API that can be
   * used for S3 ingestion.
   *
   * @return S3InputSourceConfig with temporary credentials. The
   * {@code assumeRoleArn} and {@code assumeRoleExternalId} fields are set to null
   * since MinIO does not support them.
   */
  public S3InputSourceConfig createTempCredentialsForInputSource()
  {
    ensureRunning();

    final StsClient stsClient = createStsClient();

    // SDK v2 requires a non-null roleArn. MinIO does not validate the ARN,
    // but without an inline policy the resulting session may have no permissions.
    // An explicit S3 full-access policy ensures the temp credentials work.
    final String s3FullAccessPolicy =
        "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:*\"],\"Resource\":[\"*\"]}]}";
    final AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
        .roleArn("arn:aws:iam::000000000000:role/test-role")
        .roleSessionName("test-session")
        .policy(s3FullAccessPolicy)
        .build();

    final AssumeRoleResponse result = stsClient.assumeRole(assumeRoleRequest);
    if (!result.sdkHttpResponse().isSuccessful()) {
      throw new ISE("AssumeRole request failed with code[%s]", result.sdkHttpResponse().statusCode());
    }

    final Credentials credentials = result.credentials();
    return new S3InputSourceConfig(
        new DefaultPasswordProvider(credentials.accessKeyId()),
        new DefaultPasswordProvider(credentials.secretAccessKey()),
        null,
        null,
        new DefaultPasswordProvider(credentials.sessionToken())
    );
  }

  private S3Client createS3Client()
  {
    return S3Client
        .builder()
        .endpointOverride(URI.create(getEndpointUrl()))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(getAccessKey(), getSecretKey())
        ))
        .forcePathStyle(true)
        .build();
  }

  private StsClient createStsClient()
  {
    return StsClient
        .builder()
        .endpointOverride(URI.create(getEndpointUrl()))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(getAccessKey(), getSecretKey())
        ))
        .build();
  }
}
