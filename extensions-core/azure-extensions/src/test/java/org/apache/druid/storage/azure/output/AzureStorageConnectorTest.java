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

package org.apache.druid.storage.azure.output;

import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.azure.AzureStorage;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AzureStorageConnectorTest
{
  private static final String CONTAINER = "CONTAINER";
  private static final String PREFIX = "P/R/E/F/I/X";
  public static final String TEST_FILE = "test.csv";

  private StorageConnector storageConnector;
  private final AzureStorage azureStorage = EasyMock.createMock(AzureStorage.class);

  @BeforeEach
  public void setup(@TempDir File tempDir)
  {
    storageConnector = new AzureStorageConnector(
        new AzureOutputConfig(CONTAINER, PREFIX, tempDir, null, null),
        azureStorage
    );
  }

  @Test
  public void testPathExistsSuccess() throws BlobStorageException, IOException
  {
    final Capture<String> bucket = Capture.newInstance();
    final Capture<String> path = Capture.newInstance();
    EasyMock.reset(azureStorage);
    EasyMock.expect(azureStorage.getBlockBlobExists(EasyMock.capture(bucket), EasyMock.capture(path), EasyMock.anyInt()))
            .andReturn(true);
    EasyMock.replay(azureStorage);
    assertTrue(storageConnector.pathExists(TEST_FILE));
    assertEquals(CONTAINER, bucket.getValue());
    assertEquals(PREFIX + "/" + TEST_FILE, path.getValue());
    EasyMock.verify(azureStorage);
  }

  @Test
  public void testPathExistsNotFound() throws BlobStorageException, IOException
  {
    final Capture<String> bucket = Capture.newInstance();
    final Capture<String> path = Capture.newInstance();
    EasyMock.reset(azureStorage);
    EasyMock.expect(azureStorage.getBlockBlobExists(EasyMock.capture(bucket), EasyMock.capture(path), EasyMock.anyInt()))
            .andReturn(false);
    EasyMock.replay(azureStorage);
    assertFalse(storageConnector.pathExists(TEST_FILE));
    assertEquals(CONTAINER, bucket.getValue());
    assertEquals(PREFIX + "/" + TEST_FILE, path.getValue());
    EasyMock.verify(azureStorage);
  }

  @Test
  public void testRead() throws BlobStorageException, IOException
  {
    EasyMock.reset(azureStorage);

    String data = "test";
    EasyMock.expect(azureStorage.getBlockBlobLength(EasyMock.anyString(), EasyMock.anyString()))
            .andReturn(4L);
    EasyMock.expect(
        azureStorage.getBlockBlobInputStream(
            EasyMock.anyLong(),
            EasyMock.anyLong(),
            EasyMock.anyString(),
            EasyMock.anyString(),
            EasyMock.anyInt()
        )
    ).andReturn(IOUtils.toInputStream(data, StandardCharsets.UTF_8));

    EasyMock.replay(azureStorage);
    InputStream is = storageConnector.read(TEST_FILE);
    byte[] dataBytes = new byte[data.length()];
    assertEquals(data.length(), is.read(dataBytes));
    assertEquals(-1, is.read());
    assertEquals(data, new String(dataBytes, StandardCharsets.UTF_8));

    EasyMock.reset(azureStorage);
  }

  @Test
  public void testReadRange() throws BlobStorageException, IOException
  {
    String data = "test";

    for (int start = 0; start < data.length(); ++start) {
      for (long length = 1; length <= data.length() - start; ++length) {
        String dataQueried = data.substring(start, start + ((Long) length).intValue());
        EasyMock.reset(azureStorage);
        EasyMock.expect(azureStorage.getBlockBlobInputStream(
                    EasyMock.anyLong(),
                    EasyMock.anyLong(),
                    EasyMock.anyString(),
                    EasyMock.anyString(),
                    EasyMock.anyInt()
                ))
                .andReturn(IOUtils.toInputStream(dataQueried, StandardCharsets.UTF_8));
        EasyMock.replay(azureStorage);

        InputStream is = storageConnector.readRange(TEST_FILE, start, length);
        byte[] dataBytes = new byte[((Long) length).intValue()];
        assertEquals(length, is.read(dataBytes));
        assertEquals(-1, is.read());
        assertEquals(dataQueried, new String(dataBytes, StandardCharsets.UTF_8));
        EasyMock.reset(azureStorage);
      }
    }
  }

  @Test
  public void testDeleteSinglePath() throws BlobStorageException, IOException
  {
    EasyMock.reset(azureStorage);
    Capture<String> containerCapture = EasyMock.newCapture();
    Capture<Iterable<String>> pathsCapture = EasyMock.newCapture();
    EasyMock.expect(azureStorage.batchDeleteFiles(
        EasyMock.capture(containerCapture),
        EasyMock.capture(pathsCapture),
        EasyMock.anyInt()
    )).andReturn(true);
    EasyMock.replay(azureStorage);
    storageConnector.deleteFile(TEST_FILE);
    assertEquals(CONTAINER, containerCapture.getValue());
    assertEquals(Collections.singletonList(PREFIX + "/" + TEST_FILE), pathsCapture.getValue());
    EasyMock.reset(azureStorage);
  }

  @Test
  public void testDeleteMultiplePaths() throws BlobStorageException, IOException
  {
    EasyMock.reset(azureStorage);
    Capture<String> containerCapture = EasyMock.newCapture();
    Capture<Iterable<String>> pathsCapture = EasyMock.newCapture();
    EasyMock.expect(azureStorage.batchDeleteFiles(
            EasyMock.capture(containerCapture),
            EasyMock.capture(pathsCapture),
            EasyMock.anyInt()
    )).andReturn(true);
    EasyMock.replay(azureStorage);
    storageConnector.deleteFiles(ImmutableList.of(TEST_FILE + "_1.part", TEST_FILE + "_2.part"));
    assertEquals(CONTAINER, containerCapture.getValue());
    assertEquals(
        ImmutableList.of(
            PREFIX + "/" + TEST_FILE + "_1.part",
            PREFIX + "/" + TEST_FILE + "_2.part"
        ),
        Lists.newArrayList(pathsCapture.getValue())
    );
    EasyMock.reset(azureStorage);
  }

  @Test
  public void testListDir() throws BlobStorageException, IOException
  {
    EasyMock.reset(azureStorage);
    EasyMock.expect(azureStorage.listBlobs(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyInt(), EasyMock.anyInt()))
            .andReturn(ImmutableList.of(PREFIX + "/x/y/z/" + TEST_FILE, PREFIX + "/p/q/r/" + TEST_FILE));
    EasyMock.replay(azureStorage);
    List<String> ret = Lists.newArrayList(storageConnector.listDir(""));
    assertEquals(ImmutableList.of("x/y/z/" + TEST_FILE, "p/q/r/" + TEST_FILE), ret);
    EasyMock.reset(azureStorage);
  }

  @Test
  public void test_deleteFile_blobStorageException()
  {
    EasyMock.reset(azureStorage);
    HttpResponse mockHttpResponse = EasyMock.createMock(HttpResponse.class);
    azureStorage.batchDeleteFiles(EasyMock.anyString(), EasyMock.anyObject(), EasyMock.anyInt());
    EasyMock.expectLastCall().andThrow(new BlobStorageException("error", mockHttpResponse, null));
    EasyMock.replay(azureStorage);
    assertThrows(IOException.class, () -> storageConnector.deleteFile("file"));
    EasyMock.verify(azureStorage);
    EasyMock.reset(azureStorage);
  }

  @Test
  public void test_deleteFiles_blobStorageException()
  {
    EasyMock.reset(azureStorage);
    HttpResponse mockHttpResponse = EasyMock.createMock(HttpResponse.class);
    azureStorage.batchDeleteFiles(EasyMock.anyString(), EasyMock.anyObject(), EasyMock.anyInt());
    EasyMock.expectLastCall().andThrow(new BlobStorageException("error", mockHttpResponse, null));
    EasyMock.replay(azureStorage);
    assertThrows(IOException.class, () -> storageConnector.deleteFiles(ImmutableList.of()));
    EasyMock.verify(azureStorage);
    EasyMock.reset(azureStorage);
  }
}
