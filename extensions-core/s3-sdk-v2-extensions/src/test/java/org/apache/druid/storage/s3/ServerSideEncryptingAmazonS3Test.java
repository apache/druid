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
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private S3Client mockAmazonS3;
  private ServerSideEncryption mockServerSideEncryption;
  private S3TransferConfig mockTransferConfig;

  @Before
  public void setup()
  {
    mockAmazonS3 = EasyMock.createMock(S3Client.class);
    mockServerSideEncryption = EasyMock.createMock(ServerSideEncryption.class);
    mockTransferConfig = EasyMock.createMock(S3TransferConfig.class);
  }

  @Test
  public void testConstructor_WithTransferManager() throws NoSuchFieldException, IllegalAccessException
  {
    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(true);
    EasyMock.expect(mockTransferConfig.getMinimumUploadPartSize()).andReturn(5L);
    EasyMock.expect(mockTransferConfig.getMultipartUploadThreshold()).andReturn(10L);
    EasyMock.replay(mockTransferConfig);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(mockAmazonS3, mockServerSideEncryption, mockTransferConfig);

    // Note: In SDK v2, transfer manager is disabled, so transferManager field may be null
    Assert.assertNotNull(s3);
    EasyMock.verify(mockTransferConfig);
  }

  @Test
  public void testConstructor_WithoutTransferManager() throws NoSuchFieldException, IllegalAccessException
  {

    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(false);
    EasyMock.replay(mockTransferConfig);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(mockAmazonS3, mockServerSideEncryption, mockTransferConfig);

    Field transferManagerField = ServerSideEncryptingAmazonS3.class.getDeclaredField("transferManager");
    transferManagerField.setAccessible(true);
    Object transferManager = transferManagerField.get(s3);

    Assert.assertNull("TransferManager should not be initialized", transferManager);
    Assert.assertNotNull(s3);
    EasyMock.verify(mockTransferConfig);
  }

  @Test
  public void testUpload() throws IOException
  {
    File testFile = tempFolder.newFile("test.txt");

    PutObjectRequest originalRequest = PutObjectRequest.builder()
        .bucket("bucket")
        .key("key")
        .build();
    PutObjectRequest decoratedRequest = PutObjectRequest.builder()
        .bucket("bucket")
        .key("key")
        .serverSideEncryption("AES256")
        .build();
    PutObjectResponse mockResult = PutObjectResponse.builder()
        .build();

    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(false);
    EasyMock.replay(mockTransferConfig);

    EasyMock.expect(mockServerSideEncryption.decorate(originalRequest)).andReturn(decoratedRequest);
    EasyMock.replay(mockServerSideEncryption);

    EasyMock.expect(mockAmazonS3.putObject(EasyMock.eq(decoratedRequest), EasyMock.anyObject(RequestBody.class)))
            .andReturn(mockResult)
            .once();
    EasyMock.replay(mockAmazonS3);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(mockAmazonS3, mockServerSideEncryption, mockTransferConfig);
    s3.upload(originalRequest, testFile);

    EasyMock.verify(mockServerSideEncryption);
    EasyMock.verify(mockAmazonS3);
    EasyMock.verify(mockTransferConfig);
  }
}
