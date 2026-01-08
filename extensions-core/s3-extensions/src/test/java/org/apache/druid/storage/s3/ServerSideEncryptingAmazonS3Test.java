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

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;


public class ServerSideEncryptingAmazonS3Test
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private S3Client mockS3Client;
  private ServerSideEncryption mockServerSideEncryption;
  private S3TransferConfig mockTransferConfig;

  @Before
  public void setup()
  {
    mockS3Client = EasyMock.createMock(S3Client.class);
    mockServerSideEncryption = EasyMock.createMock(ServerSideEncryption.class);
    mockTransferConfig = EasyMock.createMock(S3TransferConfig.class);
  }

  @Test
  public void testConstructor_TransferManagerNullWithoutAsyncClient() throws NoSuchFieldException, IllegalAccessException
  {
    // When no async client is provided, transferManager should be null
    // isUseTransferManager is called in constructor, so we need to set up expectation
    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(true).anyTimes();
    EasyMock.replay(mockTransferConfig);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(
        mockS3Client,
        null,  // no async client
        mockServerSideEncryption,
        mockTransferConfig
    );

    Field transferManagerField = ServerSideEncryptingAmazonS3.class.getDeclaredField("transferManager");
    transferManagerField.setAccessible(true);
    Object transferManager = transferManagerField.get(s3);

    Assert.assertNull("TransferManager should be null when no async client provided", transferManager);
    Assert.assertNotNull(s3);
    Assert.assertEquals(mockS3Client, s3.getS3Client());
  }

  @Test
  public void testUpload() throws IOException
  {
    File testFile = temporaryFolder.newFile("test-upload.txt");

    PutObjectResponse mockResponse = PutObjectResponse.builder().build();

    // Set up transfer config to return false for useTransferManager
    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(false).anyTimes();
    EasyMock.replay(mockTransferConfig);

    // decorate method takes a Builder and returns a Builder
    EasyMock.expect(mockServerSideEncryption.decorate(EasyMock.anyObject(PutObjectRequest.Builder.class)))
        .andAnswer(() -> (PutObjectRequest.Builder) EasyMock.getCurrentArguments()[0]);
    EasyMock.replay(mockServerSideEncryption);

    // The actual call is putObject(PutObjectRequest, RequestBody)
    EasyMock.expect(mockS3Client.putObject(EasyMock.anyObject(PutObjectRequest.class), EasyMock.anyObject(RequestBody.class)))
        .andReturn(mockResponse).once();
    EasyMock.replay(mockS3Client);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(
        mockS3Client,
        null,
        mockServerSideEncryption,
        mockTransferConfig
    );
    s3.upload("bucket", "key", testFile, null);

    EasyMock.verify(mockServerSideEncryption);
    EasyMock.verify(mockS3Client);
  }

  @Test
  public void testUpload_WithGrantFullControlHeaderFormatted() throws IOException
  {
    File testFile = temporaryFolder.newFile("test-upload-acl.txt");

    PutObjectResponse mockResponse = PutObjectResponse.builder().build();

    // Set up transfer config to return false for useTransferManager
    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(false).anyTimes();
    EasyMock.replay(mockTransferConfig);

    EasyMock.expect(mockServerSideEncryption.decorate(EasyMock.anyObject(PutObjectRequest.Builder.class)))
            .andAnswer(() -> (PutObjectRequest.Builder) EasyMock.getCurrentArguments()[0]);
    EasyMock.replay(mockServerSideEncryption);

    final org.easymock.Capture<PutObjectRequest> requestCapture = EasyMock.newCapture();
    EasyMock.expect(mockS3Client.putObject(EasyMock.capture(requestCapture), EasyMock.anyObject(RequestBody.class)))
            .andReturn(mockResponse).once();
    EasyMock.replay(mockS3Client);

    final Grant grant = Grant.builder()
                             .grantee(Grantee.builder().id("canonical-id").build())
                             .build();

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(
        mockS3Client,
        null,
        mockServerSideEncryption,
        mockTransferConfig
    );
    s3.upload("bucket", "key", testFile, grant);

    Assert.assertEquals("id=\"canonical-id\"", requestCapture.getValue().grantFullControl());
    EasyMock.verify(mockServerSideEncryption);
    EasyMock.verify(mockS3Client);
  }

  @Test
  public void testPutObjectWithFile() throws IOException
  {
    File testFile = temporaryFolder.newFile("test-put-object.txt");

    PutObjectResponse mockResponse = PutObjectResponse.builder().build();

    // Set up transfer config to return false for useTransferManager
    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(false).anyTimes();
    EasyMock.replay(mockTransferConfig);

    // decorate method takes a Builder and returns a Builder
    EasyMock.expect(mockServerSideEncryption.decorate(EasyMock.anyObject(PutObjectRequest.Builder.class)))
        .andAnswer(() -> (PutObjectRequest.Builder) EasyMock.getCurrentArguments()[0]);
    EasyMock.replay(mockServerSideEncryption);

    EasyMock.expect(mockS3Client.putObject(EasyMock.anyObject(PutObjectRequest.class), EasyMock.anyObject(RequestBody.class)))
        .andReturn(mockResponse).once();
    EasyMock.replay(mockS3Client);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(
        mockS3Client,
        null,
        mockServerSideEncryption,
        mockTransferConfig
    );
    PutObjectResponse response = s3.putObject("bucket", "key", testFile);

    Assert.assertNotNull(response);
    EasyMock.verify(mockServerSideEncryption);
    EasyMock.verify(mockS3Client);
  }

  @Test
  public void testConstructor_TransferManagerCreatedWithAsyncClient() throws NoSuchFieldException, IllegalAccessException
  {
    // When async client is provided and useTransferManager is true, transferManager should be created
    S3AsyncClient mockAsyncClient = EasyMock.createMock(S3AsyncClient.class);
    EasyMock.replay(mockAsyncClient);

    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(true).anyTimes();
    EasyMock.replay(mockTransferConfig);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(
        mockS3Client,
        mockAsyncClient,
        mockServerSideEncryption,
        mockTransferConfig
    );

    Field transferManagerField = ServerSideEncryptingAmazonS3.class.getDeclaredField("transferManager");
    transferManagerField.setAccessible(true);
    Object transferManager = transferManagerField.get(s3);

    Assert.assertNotNull("TransferManager should be created when async client is provided", transferManager);
    Assert.assertTrue("TransferManager should be S3TransferManager instance", transferManager instanceof S3TransferManager);
  }

  @Test
  public void testConstructor_TransferManagerNotCreatedWhenDisabled() throws NoSuchFieldException, IllegalAccessException
  {
    // When useTransferManager is false, transferManager should be null even with async client
    S3AsyncClient mockAsyncClient = EasyMock.createMock(S3AsyncClient.class);
    EasyMock.replay(mockAsyncClient);

    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(false).anyTimes();
    EasyMock.replay(mockTransferConfig);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(
        mockS3Client,
        mockAsyncClient,
        mockServerSideEncryption,
        mockTransferConfig
    );

    Field transferManagerField = ServerSideEncryptingAmazonS3.class.getDeclaredField("transferManager");
    transferManagerField.setAccessible(true);
    Object transferManager = transferManagerField.get(s3);

    Assert.assertNull("TransferManager should be null when disabled", transferManager);
  }

  @Test
  public void testBuilder() throws IOException
  {
    File testFile = temporaryFolder.newFile("test-builder.txt");

    S3Client builtClient = EasyMock.createMock(S3Client.class);
    EasyMock.replay(builtClient);

    ServerSideEncryptingAmazonS3.Builder builder = ServerSideEncryptingAmazonS3.builder()
        .setS3ClientSupplier(() -> builtClient)
        .setS3StorageConfig(new S3StorageConfig(new NoopServerSideEncryption(), null));

    ServerSideEncryptingAmazonS3 s3 = builder.build();

    Assert.assertNotNull(s3);
    Assert.assertEquals(builtClient, s3.getS3Client());
  }

  @Test
  public void testBuilderWithAsyncClient() throws NoSuchFieldException, IllegalAccessException
  {
    S3Client builtClient = EasyMock.createMock(S3Client.class);
    EasyMock.replay(builtClient);

    S3AsyncClient builtAsyncClient = EasyMock.createMock(S3AsyncClient.class);
    EasyMock.replay(builtAsyncClient);

    S3TransferConfig transferConfig = new S3TransferConfig();
    transferConfig.setUseTransferManager(true);

    ServerSideEncryptingAmazonS3.Builder builder = ServerSideEncryptingAmazonS3.builder()
        .setS3ClientSupplier(() -> builtClient)
        .setS3AsyncClientSupplier(() -> builtAsyncClient)
        .setS3StorageConfig(new S3StorageConfig(new NoopServerSideEncryption(), transferConfig));

    ServerSideEncryptingAmazonS3 s3 = builder.build();

    Assert.assertNotNull(s3);
    Assert.assertEquals(builtClient, s3.getS3Client());

    // Verify transfer manager was created
    Field transferManagerField = ServerSideEncryptingAmazonS3.class.getDeclaredField("transferManager");
    transferManagerField.setAccessible(true);
    Object transferManager = transferManagerField.get(s3);

    Assert.assertNotNull("TransferManager should be created with async client", transferManager);
  }

  @Test
  public void testUpload_UsesTransferManagerWhenAvailable() throws IOException, NoSuchFieldException, IllegalAccessException
  {
    File testFile = temporaryFolder.newFile("test-async-upload.txt");

    // Set up transfer config to return true for useTransferManager
    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(true).anyTimes();
    EasyMock.replay(mockTransferConfig);

    // Set up server side encryption mock
    EasyMock.expect(mockServerSideEncryption.decorate(EasyMock.anyObject(PutObjectRequest.Builder.class)))
        .andAnswer(() -> (PutObjectRequest.Builder) EasyMock.getCurrentArguments()[0]);
    EasyMock.replay(mockServerSideEncryption);

    // Create a mock S3AsyncClient
    S3AsyncClient mockAsyncClient = EasyMock.createMock(S3AsyncClient.class);
    EasyMock.replay(mockAsyncClient);

    // Create a mock TransferManager
    S3TransferManager mockTransferManager = EasyMock.createMock(S3TransferManager.class);

    // Create a mock FileUpload that returns a completed future
    FileUpload mockFileUpload = EasyMock.createMock(FileUpload.class);
    CompletedFileUpload completedUpload = CompletedFileUpload.builder()
        .response(software.amazon.awssdk.services.s3.model.PutObjectResponse.builder().build())
        .build();
    CompletableFuture<CompletedFileUpload> completedFuture = CompletableFuture.completedFuture(completedUpload);

    // Set up expectations for transfer manager
    Capture<UploadFileRequest> uploadRequestCapture = EasyMock.newCapture();
    EasyMock.expect(mockTransferManager.uploadFile(EasyMock.capture(uploadRequestCapture)))
        .andReturn(mockFileUpload);
    EasyMock.expect(mockFileUpload.completionFuture()).andReturn(completedFuture);
    EasyMock.replay(mockTransferManager, mockFileUpload);

    // Don't set up any expectations on mockS3Client - it should NOT be called for putObject
    // (the sync path should not be used)

    // Create the ServerSideEncryptingAmazonS3 instance
    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(
        mockS3Client,
        mockAsyncClient,
        mockServerSideEncryption,
        mockTransferConfig
    );

    // Inject the mock transfer manager
    Field transferManagerField = ServerSideEncryptingAmazonS3.class.getDeclaredField("transferManager");
    transferManagerField.setAccessible(true);
    transferManagerField.set(s3, mockTransferManager);

    // Call upload
    s3.upload("test-bucket", "test-key", testFile, null);

    // Verify the transfer manager was used
    EasyMock.verify(mockTransferManager, mockFileUpload);

    // Verify the upload request has correct bucket and key
    UploadFileRequest capturedRequest = uploadRequestCapture.getValue();
    Assert.assertEquals("test-bucket", capturedRequest.putObjectRequest().bucket());
    Assert.assertEquals("test-key", capturedRequest.putObjectRequest().key());
    Assert.assertEquals(testFile.toPath(), capturedRequest.source());
  }

  @Test
  public void testUpload_UsesTransferManagerWithAclGrant() throws IOException, NoSuchFieldException, IllegalAccessException
  {
    File testFile = temporaryFolder.newFile("test-async-upload-acl.txt");

    // Set up transfer config
    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(true).anyTimes();
    EasyMock.replay(mockTransferConfig);

    // Set up server side encryption mock
    EasyMock.expect(mockServerSideEncryption.decorate(EasyMock.anyObject(PutObjectRequest.Builder.class)))
        .andAnswer(() -> (PutObjectRequest.Builder) EasyMock.getCurrentArguments()[0]);
    EasyMock.replay(mockServerSideEncryption);

    S3AsyncClient mockAsyncClient = EasyMock.createMock(S3AsyncClient.class);
    EasyMock.replay(mockAsyncClient);

    S3TransferManager mockTransferManager = EasyMock.createMock(S3TransferManager.class);
    FileUpload mockFileUpload = EasyMock.createMock(FileUpload.class);
    CompletedFileUpload completedUpload = CompletedFileUpload.builder()
        .response(software.amazon.awssdk.services.s3.model.PutObjectResponse.builder().build())
        .build();
    CompletableFuture<CompletedFileUpload> completedFuture = CompletableFuture.completedFuture(completedUpload);

    Capture<UploadFileRequest> uploadRequestCapture = EasyMock.newCapture();
    EasyMock.expect(mockTransferManager.uploadFile(EasyMock.capture(uploadRequestCapture)))
        .andReturn(mockFileUpload);
    EasyMock.expect(mockFileUpload.completionFuture()).andReturn(completedFuture);
    EasyMock.replay(mockTransferManager, mockFileUpload);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(
        mockS3Client,
        mockAsyncClient,
        mockServerSideEncryption,
        mockTransferConfig
    );

    // Inject mock transfer manager
    Field transferManagerField = ServerSideEncryptingAmazonS3.class.getDeclaredField("transferManager");
    transferManagerField.setAccessible(true);
    transferManagerField.set(s3, mockTransferManager);

    // Create a grant
    Grant grant = Grant.builder()
        .grantee(Grantee.builder().id("test-canonical-id").build())
        .build();

    // Call upload with ACL grant
    s3.upload("test-bucket", "test-key", testFile, grant);

    // Verify transfer manager was used
    EasyMock.verify(mockTransferManager, mockFileUpload);

    // Verify the ACL grant was applied
    UploadFileRequest capturedRequest = uploadRequestCapture.getValue();
    Assert.assertEquals("id=\"test-canonical-id\"", capturedRequest.putObjectRequest().grantFullControl());
  }
}
