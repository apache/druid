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

import com.microsoft.azure.storage.StorageException;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.azure.AzureStorage;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URISyntaxException;

public class AzureStorageConnectorTest
{

  private static final String CONTAINER = "CONTAINER";
  private static final String PREFIX = "P/R/E/F/I/X";
  public static final String TEST_FILE = "test.csv";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private StorageConnector storageConnector;
  private final AzureStorage azureStorage = EasyMock.createMock(AzureStorage.class);

  @Before
  public void setup() throws IOException
  {
    storageConnector = new AzureStorageConnector(
        new AzureOutputConfig(CONTAINER, PREFIX, temporaryFolder.newFolder(), null, null),
        azureStorage
    );
  }


  @Test
  public void testPathExistsSuccess() throws URISyntaxException, StorageException, IOException
  {
    final Capture<String> bucket = Capture.newInstance();
    final Capture<String> path = Capture.newInstance();
    EasyMock.reset(azureStorage);
    EasyMock.expect(azureStorage.getBlobExists(EasyMock.capture(bucket), EasyMock.capture(path))).andReturn(true);
    EasyMock.replay(azureStorage);
    Assert.assertTrue(storageConnector.pathExists(TEST_FILE));
    Assert.assertEquals(CONTAINER, bucket.getValue());
    Assert.assertEquals(PREFIX + "/" + TEST_FILE, path.getValue());
    EasyMock.verify(azureStorage);
  }

  @Test
  public void testPathExistsErrors() throws URISyntaxException, StorageException, IOException
  {
    final Capture<String> bucket = Capture.newInstance();
    final Capture<String> path = Capture.newInstance();
    EasyMock.reset(azureStorage);
    EasyMock.expect(azureStorage.getBlobExists(EasyMock.capture(bucket), EasyMock.capture(path))).andReturn(true);
    EasyMock.replay(azureStorage);
    Assert.assertTrue(storageConnector.pathExists(TEST_FILE));
    Assert.assertEquals(CONTAINER, bucket.getValue());
    Assert.assertEquals(PREFIX + "/" + TEST_FILE, path.getValue());
    EasyMock.verify(azureStorage);
  }

  @Test
  public void testPathExistsNotFound() throws URISyntaxException, StorageException, IOException
  {
    final Capture<String> bucket = Capture.newInstance();
    final Capture<String> path = Capture.newInstance();
    EasyMock.reset(azureStorage);
    EasyMock.expect(azureStorage.getBlobExists(EasyMock.capture(bucket), EasyMock.capture(path))).andReturn(false);
    EasyMock.replay(azureStorage);
    Assert.assertFalse(storageConnector.pathExists(TEST_FILE));
    Assert.assertEquals(CONTAINER, bucket.getValue());
    Assert.assertEquals(PREFIX + "/" + TEST_FILE, path.getValue());
    EasyMock.verify(azureStorage);
  }

  @Test
  public void testRead() {}

  @Test
  public void testReadRange() {}

  @Test
  public void testDeleteSinglePath() {}

  @Test
  public void testDeleteMultiplePaths() {}

  @Test
  public void testDeleteRecursive()
  {

  }

  @Test
  public void testListDir() {}
}