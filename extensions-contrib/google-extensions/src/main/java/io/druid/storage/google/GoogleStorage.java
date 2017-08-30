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

package io.druid.storage.google;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.services.storage.Storage;

import java.io.IOException;
import java.io.InputStream;

public class GoogleStorage
{
  private final Storage storage;

  public GoogleStorage(Storage storage)
  {
    this.storage = storage;
  }

  public void insert(final String bucket, final String path, AbstractInputStreamContent mediaContent) throws IOException
   {
    Storage.Objects.Insert insertObject = storage.objects().insert(bucket, null, mediaContent);
    insertObject.setName(path);
    insertObject.getMediaHttpUploader().setDirectUploadEnabled(false);
    insertObject.execute();
  }

  public InputStream get(final String bucket, final String path) throws IOException
  {
    Storage.Objects.Get getObject = storage.objects().get(bucket, path);
    getObject.getMediaHttpDownloader().setDirectDownloadEnabled(false);
    return getObject.executeMediaAsInputStream();
  }

  public void delete(final String bucket, final String path) throws IOException
  {
    storage.objects().delete(bucket, path).execute();
  }

  public boolean exists(final String bucket, final String path)
  {
    try {
      return storage.objects().get(bucket, path).executeUsingHead().isSuccessStatusCode();
    }
    catch (Exception e) {
      return false;
    }
  }
   
  public long size(final String bucket, final String path) throws IOException
  {
    return storage.objects().get(bucket, path).execute().getSize().longValue();
  }

  public String version(final String bucket, final String path) throws IOException
  {
    return storage.objects().get(bucket, path).execute().getEtag();
  }
}
