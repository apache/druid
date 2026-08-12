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

import org.apache.druid.common.aws.AWSClientConfig;
import org.apache.druid.common.aws.AWSEndpointConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Verifies that {@code druid.storage.transfer.minimumUploadPartSize} and
 * {@code druid.storage.transfer.multipartUploadThreshold} govern how the S3 client splits an upload into parts.
 *
 * <p>Part size determines how many PUT-class requests a single upload costs, which is what the per-prefix S3
 * request-rate budget is spent on. The client exercised here is the one {@link S3StorageDruidModule} provides and
 * that {@link S3DataSegmentPusher} pushes segments through, so the part size observed here is the part size an
 * indexing task pays for every segment it writes.
 *
 * <p>The built {@code S3AsyncClient} keeps its multipart settings in SDK-internal fields, so these tests assert on
 * what S3 actually received: a multipart ETag carries a {@code -<partCount>} suffix, a single-PUT ETag does not.
 */
@Testcontainers
@Tag("requires-dockerd")
public class ServerSideEncryptingAmazonS3MultipartUploadTest
{
  private static final String BUCKET = "testbucket";
  private static final long MIB = 1024 * 1024L;

  /**
   * Deliberately different from the AWS SDK's own 8 MiB default, so a passing test means the configured value was
   * applied rather than a default that happens to be close.
   */
  private static final long CONFIGURED_PART_SIZE = 16 * MIB;

  @Container
  private static final MinIOContainer MINIO =
      new MinIOContainer(DockerImageName.parse("minio/minio:latest")).withEnv("MINIO_DOMAIN", "localhost");

  @TempDir
  public File temporaryFolder;

  private ServerSideEncryptingAmazonS3 s3;

  @BeforeEach
  public void setUp()
  {
    final S3TransferConfig transferConfig = new S3TransferConfig();
    transferConfig.setUseTransferManager(true);
    transferConfig.setMinimumUploadPartSize(CONFIGURED_PART_SIZE);
    transferConfig.setMultipartUploadThreshold(CONFIGURED_PART_SIZE);

    final AWSEndpointConfig endpointConfig = new AWSEndpointConfig()
    {
      @Override
      public String getUrl()
      {
        return MINIO.getS3URL();
      }

      @Override
      public String getSigningRegion()
      {
        return "us-east-1";
      }
    };

    // MinIO is reached by host:port, so bucket-as-subdomain addressing will not resolve.
    final AWSClientConfig clientConfig = new AWSClientConfig()
    {
      @Override
      public boolean isEnablePathStyleAccess()
      {
        return true;
      }
    };

    s3 = ServerSideEncryptingAmazonS3.builder(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(MINIO.getUserName(), MINIO.getPassword())),
        new S3StorageConfig(new NoopServerSideEncryption(), transferConfig),
        null,
        endpointConfig,
        clientConfig,
        null,
        null
    ).build();

    try {
      s3.getS3Client().headBucket(b -> b.bucket(BUCKET));
    }
    catch (S3Exception e) {
      if (e.statusCode() == 404) {
        s3.getS3Client().createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
      } else {
        throw e;
      }
    }
  }

  @Test
  public void testUploadSplitsFileIntoPartsOfTheConfiguredSize() throws IOException
  {
    final long fileSize = 3 * CONFIGURED_PART_SIZE;
    final String key = "part-size/upload.bin";

    s3.upload(BUCKET, key, fileOfSize("upload.bin", fileSize), null);

    Assertions.assertEquals(
        3,
        partCountOf(S3Utils.getSingleObjectMetadata(s3, BUCKET, key).eTag()),
        "a file of exactly three configured parts should be uploaded as three parts"
    );
  }

  @Test
  public void testUploadOfFileBelowConfiguredThresholdUsesASinglePut() throws IOException
  {
    final long fileSize = CONFIGURED_PART_SIZE - (4 * MIB);
    final String key = "threshold/upload.bin";

    s3.upload(BUCKET, key, fileOfSize("small.bin", fileSize), null);

    Assertions.assertEquals(
        1,
        partCountOf(S3Utils.getSingleObjectMetadata(s3, BUCKET, key).eTag()),
        "a file under the configured multipart threshold should not be split at all"
    );
  }

  private File fileOfSize(String name, long size) throws IOException
  {
    final File file = new File(temporaryFolder, name);
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      raf.setLength(size);
    }
    return file;
  }

  /**
   * S3 reports a multipart object's ETag as {@code <hash>-<partCount>}; an object written with a single PUT has no
   * suffix.
   */
  private static int partCountOf(String eTag)
  {
    final String unquoted = StringUtils.replace(eTag, "\"", "");
    final int dash = unquoted.lastIndexOf('-');
    return dash < 0 ? 1 : Integer.parseInt(unquoted.substring(dash + 1));
  }
}
