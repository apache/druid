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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import java.lang.reflect.Field;



public class ServerSideEncryptingAmazonS3Test
{
  private AmazonS3 mockAmazonS3;
  private ServerSideEncryption mockServerSideEncryption;
  private S3TransferConfig mockTransferConfig;
  private TransferManager mockTransferManager;

  @Before
  public void setup()
  {
    mockAmazonS3 = EasyMock.createMock(AmazonS3.class);
    mockServerSideEncryption = EasyMock.createMock(ServerSideEncryption.class);
    mockTransferConfig = EasyMock.createMock(S3TransferConfig.class);
    mockTransferManager = EasyMock.createMock(TransferManager.class);
  }

  @Test
  public void testConstructor_WithTransferManager() throws NoSuchFieldException, IllegalAccessException
  {
    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(true);
    EasyMock.expect(mockTransferConfig.getMinimumUploadPartSize()).andReturn(5L);
    EasyMock.expect(mockTransferConfig.getMultipartUploadThreshold()).andReturn(10L);
    EasyMock.replay(mockTransferConfig);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(mockAmazonS3, mockServerSideEncryption, mockTransferConfig);

    Field transferManagerField = ServerSideEncryptingAmazonS3.class.getDeclaredField("transferManager");
    transferManagerField.setAccessible(true);
    Object transferManager = transferManagerField.get(s3);

    Assert.assertNotNull("TransferManager should be initialized", transferManager);
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
  public void testUpload_WithoutTransferManager() throws InterruptedException
  {
    PutObjectRequest originalRequest = new PutObjectRequest("bucket", "key", "file");
    PutObjectRequest decoratedRequest = new PutObjectRequest("bucket", "key", "file-encrypted");
    PutObjectResult mockResult = new PutObjectResult();

    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(false);
    EasyMock.replay(mockTransferConfig);

    EasyMock.expect(mockServerSideEncryption.decorate(originalRequest)).andReturn(decoratedRequest);
    EasyMock.replay(mockServerSideEncryption);

    EasyMock.expect(mockAmazonS3.putObject(decoratedRequest)).andReturn(mockResult).once();
    EasyMock.replay(mockAmazonS3);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(mockAmazonS3, mockServerSideEncryption, mockTransferConfig);
    s3.upload(originalRequest);

    EasyMock.verify(mockServerSideEncryption);
    EasyMock.verify(mockAmazonS3);
    EasyMock.verify(mockTransferConfig);
  }

  @Test
  public void testUpload_WithTransferManager() throws InterruptedException, NoSuchFieldException, IllegalAccessException
  {
    PutObjectRequest originalRequest = new PutObjectRequest("bucket", "key", "file");
    PutObjectRequest decoratedRequest = new PutObjectRequest("bucket", "key", "file-encrypted");
    Upload mockUpload = EasyMock.createMock(Upload.class);

    EasyMock.expect(mockTransferConfig.isUseTransferManager()).andReturn(true).once();
    EasyMock.expect(mockTransferConfig.getMinimumUploadPartSize()).andReturn(5242880L).once(); // 5 MB
    EasyMock.expect(mockTransferConfig.getMultipartUploadThreshold()).andReturn(10485760L).once(); // 10 MB
    EasyMock.replay(mockTransferConfig);

    EasyMock.expect(mockServerSideEncryption.decorate(originalRequest)).andReturn(decoratedRequest);
    EasyMock.replay(mockServerSideEncryption);

    EasyMock.expect(mockTransferManager.upload(decoratedRequest)).andReturn(mockUpload);
    EasyMock.replay(mockTransferManager);

    mockUpload.waitForCompletion();
    EasyMock.expectLastCall();
    EasyMock.replay(mockUpload);

    ServerSideEncryptingAmazonS3 s3 = new ServerSideEncryptingAmazonS3(mockAmazonS3, mockServerSideEncryption, mockTransferConfig);

    Field transferManagerField = ServerSideEncryptingAmazonS3.class.getDeclaredField("transferManager");
    transferManagerField.setAccessible(true);
    transferManagerField.set(s3, mockTransferManager);

    s3.upload(originalRequest);

    EasyMock.verify(mockServerSideEncryption);
    EasyMock.verify(mockTransferManager);
    EasyMock.verify(mockUpload);
  }
}
