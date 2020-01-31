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

import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.storage.azure.AzureByteSourceFactory;
import org.apache.druid.storage.azure.AzureStorage;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

public class AzureEntityTest extends EasyMockSupport
{
  private static final String CONTAINER_NAME = "container";
  private static final String BLOB_NAME = "blob";
  private static final URI ENTITY_URI;
  private AzureStorage storage;
  private CloudObjectLocation location;
  private AzureByteSourceFactory byteSourceFactory;

  private AzureEntity azureEntity;

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
    storage = createMock(AzureStorage.class);
    location = createMock(CloudObjectLocation.class);
    byteSourceFactory = createMock(AzureByteSourceFactory.class);
    azureEntity = new AzureEntity(storage, location, byteSourceFactory);
  }

  @Test
  public void test_getUri_returnsLocationUri()
  {
    EasyMock.expect(location.toUri(AzureInputSource.SCHEME)).andReturn(ENTITY_URI);
    URI actualUri = azureEntity.getUri();
    Assert.assertEquals(ENTITY_URI, actualUri);

  }

}
