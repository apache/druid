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

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;


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
  public void testConstructor_TransferManagerAlwaysNull() throws NoSuchFieldException, IllegalAccessException
  {
    // In SDK v2 implementation, transferManager is always null for simplicity
    // since S3TransferManager requires an async client
    EasyMock.replay(mockTransferConfig);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(mockS3Client, mockServerSideEncryption, mockTransferConfig);

    Field transferManagerField = ServerSideEncryptingAmazonS3.class.getDeclaredField("transferManager");
    transferManagerField.setAccessible(true);
    Object transferManager = transferManagerField.get(s3);

    // TransferManager is always null in the current implementation
    Assert.assertNull("TransferManager should be null (disabled for sync operations)", transferManager);
    Assert.assertNotNull(s3);
    Assert.assertEquals(mockS3Client, s3.getS3Client());
  }

  @Test
  public void testUpload() throws IOException
  {
    File testFile = temporaryFolder.newFile("test-upload.txt");

    PutObjectResponse mockResponse = PutObjectResponse.builder().build();

    EasyMock.replay(mockTransferConfig);

    // decorate method takes a Builder and returns a Builder
    EasyMock.expect(mockServerSideEncryption.decorate(EasyMock.anyObject(PutObjectRequest.Builder.class)))
        .andAnswer(() -> (PutObjectRequest.Builder) EasyMock.getCurrentArguments()[0]);
    EasyMock.replay(mockServerSideEncryption);

    // The actual call is putObject(PutObjectRequest, RequestBody)
    EasyMock.expect(mockS3Client.putObject(EasyMock.anyObject(PutObjectRequest.class), EasyMock.anyObject(RequestBody.class)))
        .andReturn(mockResponse).once();
    EasyMock.replay(mockS3Client);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(mockS3Client, mockServerSideEncryption, mockTransferConfig);
    s3.upload("bucket", "key", testFile, null);

    EasyMock.verify(mockServerSideEncryption);
    EasyMock.verify(mockS3Client);
  }

  @Test
  public void testPutObjectWithFile() throws IOException
  {
    File testFile = temporaryFolder.newFile("test-put-object.txt");

    PutObjectResponse mockResponse = PutObjectResponse.builder().build();

    EasyMock.replay(mockTransferConfig);

    // decorate method takes a Builder and returns a Builder
    EasyMock.expect(mockServerSideEncryption.decorate(EasyMock.anyObject(PutObjectRequest.Builder.class)))
        .andAnswer(() -> (PutObjectRequest.Builder) EasyMock.getCurrentArguments()[0]);
    EasyMock.replay(mockServerSideEncryption);

    EasyMock.expect(mockS3Client.putObject(EasyMock.anyObject(PutObjectRequest.class), EasyMock.anyObject(RequestBody.class)))
        .andReturn(mockResponse).once();
    EasyMock.replay(mockS3Client);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(mockS3Client, mockServerSideEncryption, mockTransferConfig);
    PutObjectResponse response = s3.putObject("bucket", "key", testFile);

    Assert.assertNotNull(response);
    EasyMock.verify(mockServerSideEncryption);
    EasyMock.verify(mockS3Client);
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
}
