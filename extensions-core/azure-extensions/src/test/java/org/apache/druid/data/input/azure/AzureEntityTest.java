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

package org.apache.druid.data.input.azure;

import com.google.common.base.Predicate;
import org.apache.commons.io.input.NullInputStream;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.storage.azure.AzureByteSource;
import org.apache.druid.storage.azure.AzureByteSourceFactory;
import org.apache.druid.storage.azure.AzureStorage;
import org.apache.druid.storage.azure.AzureUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class AzureEntityTest extends EasyMockSupport
{
  private static final String STORAGE_ACCOUNT_NAME = "storageAccount";
  private static final String CONTAINER_NAME = "container";
  private static final String BLOB_NAME = "blob";
  private static final int OFFSET = 20;
  private static final InputStream INPUT_STREAM = new NullInputStream(OFFSET);
  private static final IOException IO_EXCEPTION = new IOException();
  private static final URI ENTITY_URI;

  private CloudObjectLocation location;
  private AzureByteSourceFactory byteSourceFactory;
  private AzureByteSource byteSource;

  private AzureEntity azureEntity;
  private AzureStorage azureStorage;

  static {
    try {
      ENTITY_URI = new URI(AzureInputSource.SCHEME + "://" + CONTAINER_NAME + "/" + BLOB_NAME);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setup()
  {
    location = createMock(CloudObjectLocation.class);
    byteSourceFactory = createMock(AzureByteSourceFactory.class);
    byteSource = createMock(AzureByteSource.class);
    azureStorage = createMock(AzureStorage.class);
  }

  @Test
  public void test_getUri_returnsLocationUri()
  {
    EasyMock.expect(location.getBucket()).andReturn(CONTAINER_NAME);
    EasyMock.expect(location.getPath()).andReturn(BLOB_NAME);
    EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, BLOB_NAME, azureStorage)).andReturn(byteSource);
    EasyMock.expect(location.toUri(AzureInputSource.SCHEME)).andReturn(ENTITY_URI);
    replayAll();

    azureEntity = new AzureEntity(location, azureStorage, AzureInputSource.SCHEME, byteSourceFactory);

    URI actualUri = azureEntity.getUri();
    Assert.assertEquals(ENTITY_URI, actualUri);

    verifyAll();

  }

  @Test
  public void test_getUri_returnsLocationUri_azureStorageScheme()
  {
    EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, BLOB_NAME, azureStorage)).andReturn(byteSource);
    replayAll();

    azureEntity = new AzureEntity(
        new CloudObjectLocation(STORAGE_ACCOUNT_NAME, CONTAINER_NAME + "/" + BLOB_NAME),
        azureStorage,
        AzureStorageAccountInputSource.SCHEME,
        byteSourceFactory
    );

    Assert.assertEquals(
        URI.create(AzureStorageAccountInputSource.SCHEME + "://" + STORAGE_ACCOUNT_NAME + "/" + CONTAINER_NAME + "/" + BLOB_NAME),
        azureEntity.getUri()
    );

    verifyAll();

  }

  @Test
  public void test_readFromStart_returnsExpectedStream() throws Exception
  {
    EasyMock.expect(location.getBucket()).andReturn(CONTAINER_NAME);
    EasyMock.expect(location.getPath()).andReturn(BLOB_NAME);
    EasyMock.expect(byteSource.openStream(0)).andReturn(INPUT_STREAM);
    EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, BLOB_NAME, azureStorage)).andReturn(byteSource);
    replayAll();

    azureEntity = new AzureEntity(location, azureStorage, AzureInputSource.SCHEME, byteSourceFactory);

    InputStream actualInputStream = azureEntity.readFrom(0);
    Assert.assertSame(INPUT_STREAM, actualInputStream);
  }

  @Test
  public void test_readFrom_returnsExpectedStream() throws Exception
  {
    EasyMock.expect(location.getBucket()).andReturn(CONTAINER_NAME);
    EasyMock.expect(location.getPath()).andReturn(BLOB_NAME);
    EasyMock.expect(byteSource.openStream(OFFSET)).andReturn(INPUT_STREAM);
    EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, BLOB_NAME, azureStorage)).andReturn(byteSource);
    replayAll();

    azureEntity = new AzureEntity(location, azureStorage, AzureInputSource.SCHEME, byteSourceFactory);

    InputStream actualInputStream = azureEntity.readFrom(OFFSET);
    Assert.assertSame(INPUT_STREAM, actualInputStream);
  }

  @Test
  public void test_readFrom_throwsIOException_propogatesError()
  {
    try {
      EasyMock.expect(location.getBucket()).andReturn(CONTAINER_NAME);
      EasyMock.expect(location.getPath()).andReturn(BLOB_NAME);
      EasyMock.expect(byteSource.openStream(OFFSET)).andThrow(IO_EXCEPTION);
      EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, BLOB_NAME, azureStorage)).andReturn(byteSource);
      replayAll();

      azureEntity = new AzureEntity(location, azureStorage, AzureInputSource.SCHEME, byteSourceFactory);
      azureEntity.readFrom(OFFSET);
    }
    catch (IOException e) {
      verifyAll();
    }
  }

  @Test
  public void test_getPath_returnsLocationPath()
  {
    EasyMock.expect(location.getBucket()).andReturn(CONTAINER_NAME);
    EasyMock.expect(location.getPath()).andReturn(BLOB_NAME).atLeastOnce();
    EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, BLOB_NAME, azureStorage)).andReturn(byteSource);
    replayAll();

    azureEntity = new AzureEntity(location, azureStorage, AzureInputSource.SCHEME, byteSourceFactory);
    String actualPath = azureEntity.getPath();

    Assert.assertEquals(BLOB_NAME, actualPath);
    verifyAll();
  }

  @Test
  public void test_getPath_azureStorageScheme()
  {
    EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, BLOB_NAME, azureStorage)).andReturn(byteSource);
    replayAll();

    azureEntity = new AzureEntity(
        new CloudObjectLocation(STORAGE_ACCOUNT_NAME, CONTAINER_NAME + "/" + BLOB_NAME),
        azureStorage,
        AzureStorageAccountInputSource.SCHEME,
        byteSourceFactory
    );

    Assert.assertEquals(
        CONTAINER_NAME + "/" + BLOB_NAME,
        azureEntity.getPath()
    );

    verifyAll();
  }
  @Test
  public void test_getRetryCondition_returnsExpectedRetryCondition()
  {
    EasyMock.expect(location.getBucket()).andReturn(CONTAINER_NAME);
    EasyMock.expect(location.getPath()).andReturn(BLOB_NAME).atLeastOnce();
    EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, BLOB_NAME, azureStorage)).andReturn(byteSource);
    replayAll();

    azureEntity = new AzureEntity(location, azureStorage, AzureInputSource.SCHEME, byteSourceFactory);
    Predicate<Throwable> actualRetryCondition = azureEntity.getRetryCondition();
    Assert.assertSame(AzureUtils.AZURE_RETRY, actualRetryCondition);
  }
}
