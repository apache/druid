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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Collectors;

public class S3StorageConnectorTest
{
  private static final AmazonS3Client S3_CLIENT = EasyMock.createMock(AmazonS3Client.class);
  private static final ServerSideEncryptingAmazonS3 SERVICE = new ServerSideEncryptingAmazonS3(
      S3_CLIENT,
      new NoopServerSideEncryption()
  );

  private static final String BUCKET = "BUCKET";
  private static final String PREFIX = "P/R/E/F/I/X";
  public static final String TEST_FILE = "test.csv";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public static final ListObjectsV2Result TEST_RESULT = EasyMock.createMock(ListObjectsV2Result.class);


  private StorageConnector storageConnector;

  @Before
  public void setup()
  {
    try {
      storageConnector = new S3StorageConnector(new S3OutputConfig(
          BUCKET,
          PREFIX,
          temporaryFolder.newFolder(),
          null,
          null,
          null,
          true
      ), SERVICE);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  @Test
  public void pathExists() throws IOException
  {
    EasyMock.reset(S3_CLIENT);
    EasyMock.expect(S3_CLIENT.doesObjectExist(BUCKET, PREFIX + "/" + TEST_FILE)).andReturn(true);
    EasyMock.replay(S3_CLIENT);
    Assert.assertTrue(storageConnector.pathExists(TEST_FILE));
    EasyMock.reset(S3_CLIENT);
    Assert.assertFalse(storageConnector.pathExists("test1.csv"));
  }


  @Test
  public void pathRead() throws IOException
  {
    EasyMock.reset(S3_CLIENT);
    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream("test".getBytes(StandardCharsets.UTF_8)));
    EasyMock.expect(S3_CLIENT.getObject(new GetObjectRequest(BUCKET, PREFIX + "/" + TEST_FILE))).andReturn(s3Object);
    EasyMock.replay(S3_CLIENT);

    String readText = new BufferedReader(
        new InputStreamReader(storageConnector.read(TEST_FILE), StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.joining("\n"));

    Assert.assertEquals("test", readText);
    EasyMock.reset(S3_CLIENT);
  }

  @Test
  public void pathDelete() throws IOException
  {
    EasyMock.reset(S3_CLIENT);
    S3_CLIENT.deleteObject(BUCKET, PREFIX + "/" + TEST_FILE);
    EasyMock.expectLastCall();
    storageConnector.deleteFile(TEST_FILE);
    EasyMock.reset(S3_CLIENT);
  }

  @Test
  public void pathDeleteRecursively() throws IOException
  {
    EasyMock.reset(S3_CLIENT, TEST_RESULT);

    S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
    s3ObjectSummary.setBucketName(BUCKET);
    s3ObjectSummary.setKey(PREFIX + "/test/" + TEST_FILE);

    EasyMock.expect(TEST_RESULT.getObjectSummaries()).andReturn(Collections.singletonList(s3ObjectSummary)).times(2);
    EasyMock.expect(TEST_RESULT.isTruncated()).andReturn(false);
    EasyMock.expect(S3_CLIENT.listObjectsV2((ListObjectsV2Request) EasyMock.anyObject()))
            .andReturn(TEST_RESULT);

    Capture<DeleteObjectsRequest> capturedArgument = EasyMock.newCapture();
    EasyMock.expect(S3_CLIENT.deleteObjects(EasyMock.and(
        EasyMock.capture(capturedArgument),
        EasyMock.isA(DeleteObjectsRequest.class)
    ))).andReturn(null);
    EasyMock.replay(S3_CLIENT, TEST_RESULT);

    storageConnector.deleteRecursively("test");

    Assert.assertEquals(1, capturedArgument.getValue().getKeys().size());
    Assert.assertEquals(PREFIX + "/test/" + TEST_FILE, capturedArgument.getValue().getKeys().get(0).getKey());
    EasyMock.reset(S3_CLIENT, TEST_RESULT);
  }

}
