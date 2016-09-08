/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.cloudfiles;

import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import org.jclouds.io.Payload;

import java.io.IOException;
import java.io.InputStream;

public class CloudFilesByteSource extends ByteSource
{

  final private CloudFilesObjectApiProxy objectApi;
  final private String path;
  private Payload payload;

  public CloudFilesByteSource(CloudFilesObjectApiProxy objectApi, String path)
  {
    this.objectApi = objectApi;
    this.path = path;
    this.payload = null;
  }

  public void closeStream() throws IOException
  {
    if (payload != null) {
      payload.close();
      payload = null;
    }
  }

  @Override
  public InputStream openStream() throws IOException
  {
    payload = (payload == null) ? objectApi.get(path).getPayload() : payload;

    try {
      return payload.openStream();
    }
    catch (IOException e) {
      if (CloudFilesUtils.CLOUDFILESRETRY.apply(e)) {
        throw new IOException("Recoverable exception", e);
      }
      throw Throwables.propagate(e);
    }
  }
}
