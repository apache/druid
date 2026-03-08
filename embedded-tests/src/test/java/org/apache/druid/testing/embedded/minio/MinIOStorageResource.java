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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.druid.common.aws.AWSModule;
import org.apache.druid.data.input.s3.S3InputSourceConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.containers.MinIOContainer;

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
  private AmazonS3 s3Client;

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
    s3Client.createBucket(bucket);

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

  public AmazonS3 getS3Client()
  {
    ensureRunning();
    return s3Client;
  }

  /**
   * Creates temporary S3 credentials using the {@link AssumeRoleRequest} that
   * can be used for S3 ingestion.
   *
   * @return S3InputSourceConfig with temporary credentials. The
   * {@code assumeRoleArn} and {@code assumeRoleExternalId} fields are set to null
   * since Min IO does not support them.
   */
  public S3InputSourceConfig createTempCredentialsForInputSource()
  {
    ensureRunning();

    final AWSSecurityTokenService stsClient = createSTSClient();

    // assumeRoleArn and assumeRoleExternalId need not be specified since MinIO ignores them
    final AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest()
        .withRoleSessionName("test-session");

    final AssumeRoleResult result = stsClient.assumeRole(assumeRoleRequest);
    final int statusCode = result.getSdkHttpMetadata().getHttpStatusCode();
    if (statusCode < 200 || statusCode >= 300) {
      throw new ISE("AssumeRole request failed with code[%s]: %s", statusCode, result.getAssumedRoleUser());
    }

    final Credentials credentials = stsClient.assumeRole(assumeRoleRequest).getCredentials();
    return new S3InputSourceConfig(
        new DefaultPasswordProvider(credentials.getAccessKeyId()),
        new DefaultPasswordProvider(credentials.getSecretAccessKey()),
        null,
        null,
        new DefaultPasswordProvider(credentials.getSessionToken())
    );
  }

  private AmazonS3 createS3Client()
  {
    return AmazonS3Client
        .builder()
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(getEndpointUrl(), "us-east-1"))
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(getAccessKey(), getSecretKey())))
        .withPathStyleAccessEnabled(true)
        .build();
  }

  private AWSSecurityTokenService createSTSClient()
  {
    return AWSSecurityTokenServiceClientBuilder
        .standard()
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(getEndpointUrl(), "us-east-1"))
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(getAccessKey(), getSecretKey())))
        .build();
  }
}
