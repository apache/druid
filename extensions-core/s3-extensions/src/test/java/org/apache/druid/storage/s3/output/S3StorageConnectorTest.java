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

package org.apache.druid.storage.s3.output;

import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.druid.storage.s3.output.S3StorageConnector.JOINER;

@Testcontainers
@Tag("requires-dockerd")
public class S3StorageConnectorTest
{
  private static final String BUCKET = "testbucket";
  private static final String PREFIX = "P/R/E/F/I/X";
  public static final String TEST_FILE = "test.csv";
  @Container
  private static final MinIOContainer MINIO = MinioUtil.createContainer();
  @TempDir
  public static File temporaryFolder;
  private ServerSideEncryptingAmazonS3 s3Client;

  private StorageConnector storageConnector;

  @BeforeEach
  public void setup() throws IOException
  {
    s3Client = MinioUtil.createS3Client(MINIO);
    if (!s3Client.getAmazonS3().doesBucketExistV2(BUCKET)) {
      s3Client.getAmazonS3().createBucket(new CreateBucketRequest(BUCKET));
    }

    S3OutputConfig s3OutputConfig = new S3OutputConfig(
        BUCKET,
        PREFIX,
        Files.createDirectory(Paths.get(temporaryFolder.getAbsolutePath(), UUID.randomUUID().toString())).toFile(),
        null,
        null,
        true
    );
    storageConnector = new S3StorageConnector(
        s3OutputConfig,
        s3Client,
        new S3UploadManager(
            s3OutputConfig,
            new S3ExportConfig("tempDir", new HumanReadableBytes("5MiB"), 1, null),
            new DruidProcessingConfigTest.MockRuntimeInfo(10, 0, 0),
            new StubServiceEmitter()
        )
    );
  }

  @Test
  public void pathExists_yes() throws IOException
  {
    s3Client.putObject(
        BUCKET,
        JOINER.join(PREFIX, TEST_FILE),
        Files.createFile(Path.of(temporaryFolder.toPath().toString(), TEST_FILE)).toFile()
    );
    Assertions.assertTrue(storageConnector.pathExists(TEST_FILE));
  }

  @Test
  public void pathExists_notFound() throws IOException
  {
    Assertions.assertFalse(storageConnector.pathExists(UUID.randomUUID().toString()));
  }

  @Test
  public void pathExists_error() throws IOException
  {
    S3OutputConfig s3OutputConfig = new S3OutputConfig(
        BUCKET,
        PREFIX,
        Files.createDirectory(Paths.get(temporaryFolder.getAbsolutePath(), UUID.randomUUID().toString())).toFile(),
        null,
        null,
        true
    );
    StorageConnector unauthorizedStorageConnector = new S3StorageConnector(
        s3OutputConfig,
        MinioUtil.createUnauthorizedS3Client(MINIO),
        new S3UploadManager(
            s3OutputConfig,
            new S3ExportConfig("tempDir", new HumanReadableBytes("5MiB"), 1, null),
            new DruidProcessingConfigTest.MockRuntimeInfo(10, 0, 0),
            new StubServiceEmitter()
        )
    );
    final IOException e2 = Assertions.assertThrows(
        IOException.class,
        () -> unauthorizedStorageConnector.pathExists(TEST_FILE)
    );
    Assertions.assertEquals(AmazonS3Exception.class, e2.getCause().getClass());
    AmazonS3Exception amazonS3Exception = (AmazonS3Exception) e2.getCause();
    Assertions.assertEquals(403, amazonS3Exception.getStatusCode());
  }

  @Test
  public void pathRead() throws IOException
  {
    try (OutputStream outputStream = storageConnector.write("readWrite1")) {
      outputStream.write("test".getBytes(StandardCharsets.UTF_8));
    }
    try (InputStream inputStream = storageConnector.read("readWrite1")) {
      byte[] bytes = inputStream.readAllBytes();
      Assertions.assertEquals("test", new String(bytes, StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testReadRange() throws IOException
  {
    String data = "test";

    try (OutputStream outputStream = storageConnector.write("readWrite2")) {
      outputStream.write(data.getBytes(StandardCharsets.UTF_8));
    }

    // non empty reads
    for (int start = 0; start < data.length(); start++) {
      for (int length = 1; length <= data.length() - start; length++) {
        String dataQueried = data.substring(start, start + length);
        try (InputStream inputStream = storageConnector.readRange("readWrite2", start, length)) {
          byte[] bytes = inputStream.readAllBytes();
          Assertions.assertEquals(dataQueried, new String(bytes, StandardCharsets.UTF_8));
        }
      }
    }

    // empty read
    try (InputStream inputStream = storageConnector.readRange("readWrite2", 0, 0)) {
      byte[] bytes = inputStream.readAllBytes();
      Assertions.assertEquals("", new String(bytes, StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testDeleteSinglePath() throws IOException
  {
    String deleteFolderName = UUID.randomUUID().toString();
    try (OutputStream outputStream = storageConnector.write(StringUtils.format("%s/deleteSingle", deleteFolderName))) {
      outputStream.write("delete".getBytes(StandardCharsets.UTF_8));
    }

    ArrayList<String> listResult = new ArrayList<>();
    storageConnector.listDir(deleteFolderName + "/").forEachRemaining(listResult::add);
    Assertions.assertEquals(1, listResult.size());
    Assertions.assertEquals("deleteSingle", listResult.get(0));
    storageConnector.deleteFile(StringUtils.format("%s/deleteSingle", deleteFolderName));

    listResult.clear();
    storageConnector.listDir(deleteFolderName + "/").forEachRemaining(listResult::add);
    Assertions.assertEquals(0, listResult.size());
  }

  @Test
  public void testDeleteMultiplePaths() throws IOException
  {
    String deleteFolderName = UUID.randomUUID().toString();
    try (OutputStream outputStream = storageConnector.write(StringUtils.format("%s/deleteFirst", deleteFolderName))) {
      outputStream.write("first".getBytes(StandardCharsets.UTF_8));
    }
    try (OutputStream outputStream = storageConnector.write(StringUtils.format("%s/deleteSecond", deleteFolderName))) {
      outputStream.write("second".getBytes(StandardCharsets.UTF_8));
    }

    ArrayList<String> listResult = new ArrayList<>();
    storageConnector.listDir(deleteFolderName + "/").forEachRemaining(listResult::add);
    Assertions.assertEquals(2, listResult.size());
    Assertions.assertEquals("deleteFirst", listResult.get(0));
    Assertions.assertEquals("deleteSecond", listResult.get(1));

    storageConnector.deleteFiles(ImmutableList.of(
        StringUtils.format("%s/deleteFirst", deleteFolderName),
        StringUtils.format("%s/deleteSecond", deleteFolderName)
    ));

    listResult.clear();
    storageConnector.listDir(deleteFolderName + "/").forEachRemaining(listResult::add);
    Assertions.assertEquals(0, listResult.size());
  }

  @Test
  public void testPathDeleteRecursively() throws IOException
  {
    String deleteFolderName = UUID.randomUUID().toString();
    try (OutputStream outputStream = storageConnector.write(StringUtils.format("%s/deleteFirst", deleteFolderName))) {
      outputStream.write("first".getBytes(StandardCharsets.UTF_8));
    }
    try (OutputStream outputStream = storageConnector.write(StringUtils.format("%s/inner/deleteSecond", deleteFolderName))) {
      outputStream.write("second".getBytes(StandardCharsets.UTF_8));
    }

    ArrayList<String> listResult = new ArrayList<>();
    storageConnector.listDir(deleteFolderName + "/").forEachRemaining(listResult::add);
    Assertions.assertEquals(2, listResult.size());
    Assertions.assertEquals("deleteFirst", listResult.get(0));
    Assertions.assertEquals("inner/deleteSecond", listResult.get(1));

    storageConnector.deleteRecursively(deleteFolderName);

    listResult.clear();
    storageConnector.listDir(deleteFolderName + "/").forEachRemaining(listResult::add);
    Assertions.assertEquals(0, listResult.size());
  }

  @Test
  public void testListDir() throws IOException
  {
    String listFolderName = UUID.randomUUID().toString();
    try (OutputStream outputStream = storageConnector.write(StringUtils.format("%s/listFirst", listFolderName))) {
      outputStream.write("first".getBytes(StandardCharsets.UTF_8));
    }
    List<String> listDirResult = Lists.newArrayList(storageConnector.listDir(listFolderName + "/"));
    Assertions.assertEquals(ImmutableList.of("listFirst"), listDirResult);
  }

  // adapted from apache iceberg tests
  private static class MinioUtil
  {
    private MinioUtil()
    {
    }

    public static MinIOContainer createContainer()
    {
      return createContainer(null);
    }

    public static MinIOContainer createContainer(AWSCredentials credentials)
    {
      MinIOContainer container = new MinIOContainer(DockerImageName.parse("minio/minio:latest"));

      // this enables virtual-host-style requests. see
      // https://github.com/minio/minio/tree/master/docs/config#domain
      container.withEnv("MINIO_DOMAIN", "localhost");

      if (credentials != null) {
        container.withUserName(credentials.getAWSAccessKeyId());
        container.withPassword(credentials.getAWSSecretKey());
      }

      return container;
    }

    public static ServerSideEncryptingAmazonS3 createS3Client(MinIOContainer container)
    {
      final AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3Client
          .builder()
          .withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(
                  container.getS3URL(),
                  "us-east-1"
              )
          )
          .withCredentials(new StaticCredentialsProvider(
              new BasicAWSCredentials(container.getUserName(), container.getPassword())))
          .withClientConfiguration(new ClientConfigurationFactory().getConfig())
          .withPathStyleAccessEnabled(true); // OSX won't resolve subdomains

      return ServerSideEncryptingAmazonS3.builder()
                                         .setAmazonS3ClientBuilder(amazonS3ClientBuilder)
                                         .build();
    }

    public static ServerSideEncryptingAmazonS3 createUnauthorizedS3Client(MinIOContainer container)
    {
      final AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3Client
          .builder()
          .withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(
                  container.getS3URL(),
                  "us-east-1"
              )
          )
          .withCredentials(new StaticCredentialsProvider(
              new BasicAWSCredentials(container.getUserName(), "wrong")))
          .withClientConfiguration(new ClientConfigurationFactory().getConfig())
          .withPathStyleAccessEnabled(true); // OSX won't resolve subdomains

      return ServerSideEncryptingAmazonS3.builder()
                                         .setAmazonS3ClientBuilder(amazonS3ClientBuilder)
                                         .build();
    }
  }
}
