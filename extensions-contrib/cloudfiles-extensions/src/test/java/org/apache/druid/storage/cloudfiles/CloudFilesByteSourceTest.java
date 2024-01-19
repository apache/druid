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

package org.apache.druid.storage.cloudfiles;

import org.jclouds.io.Payload;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CloudFilesByteSourceTest
{
  @Test
  public void openStreamTest() throws IOException
  {
    final String path = "path";

    CloudFilesObjectApiProxy objectApi = mock(CloudFilesObjectApiProxy.class);
    CloudFilesObject cloudFilesObject = mock(CloudFilesObject.class);
    Payload payload = mock(Payload.class);
    InputStream stream = mock(InputStream.class);

    when(objectApi.get(path, 0)).thenReturn(cloudFilesObject);
    when(cloudFilesObject.getPayload()).thenReturn(payload);
    when(payload.openStream()).thenReturn(stream);
    payload.close();

    CloudFilesByteSource byteSource = new CloudFilesByteSource(objectApi, path);
    Assert.assertEquals(stream, byteSource.openStream());
    byteSource.closeStream();

    verify(objectApi).get(path, 0);
    verify(cloudFilesObject).getPayload();
    verify(payload).openStream();
  }

  @Test()
  public void openStreamWithRecoverableErrorTest() throws IOException
  {
    final String path = "path";

    CloudFilesObjectApiProxy objectApi = mock(CloudFilesObjectApiProxy.class);
    CloudFilesObject cloudFilesObject = mock(CloudFilesObject.class);
    Payload payload = mock(Payload.class);
    InputStream stream = mock(InputStream.class);

    when(objectApi.get(path, 0)).thenReturn(cloudFilesObject);
    when(cloudFilesObject.getPayload()).thenReturn(payload);
    when(payload.openStream()).thenThrow(new IOException())
                              .thenReturn(stream);
    payload.close();

    CloudFilesByteSource byteSource = new CloudFilesByteSource(objectApi, path);
    try {
      byteSource.openStream();
    }
    catch (Exception e) {
      Assert.assertEquals("Recoverable exception", e.getMessage());
    }

    Assert.assertEquals(stream, byteSource.openStream());
    byteSource.closeStream();

    verify(objectApi).get(path, 0);
    verify(cloudFilesObject).getPayload();
    verify(payload, times(2)).openStream();
  }
}
