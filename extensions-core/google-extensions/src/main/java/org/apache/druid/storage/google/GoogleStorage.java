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

package org.apache.druid.storage.google;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Get;
import com.google.common.base.Supplier;

import java.io.IOException;
import java.io.InputStream;

public class GoogleStorage
{
  /**
   * Some segment processing tools such as DataSegmentKiller are initialized when an ingestion job starts
   * if the extension is loaded, even when the implementation of DataSegmentKiller is not used. As a result,
   * if we have a Storage instead of a supplier of it, it can cause unnecessary config validation
   * against Google storage even when it's not used at all. To perform the config validation
   * only when it is actually used, we use a supplier.
   *
   * See OmniDataSegmentKiller for how DataSegmentKillers are initialized.
   */
  private final Supplier<Storage> storage;

  public GoogleStorage(Supplier<Storage> storage)
  {
    this.storage = storage;
  }

  public void insert(final String bucket, final String path, AbstractInputStreamContent mediaContent) throws IOException
  {
    Storage.Objects.Insert insertObject = storage.get().objects().insert(bucket, null, mediaContent);
    insertObject.setName(path);
    insertObject.getMediaHttpUploader().setDirectUploadEnabled(false);
    insertObject.execute();
  }

  public InputStream get(final String bucket, final String path) throws IOException
  {
    return get(bucket, path, 0);
  }

  public InputStream get(final String bucket, final String path, long start) throws IOException
  {
    final Get get = storage.get().objects().get(bucket, path);
    InputStream inputStream = get.executeMediaAsInputStream();
    inputStream.skip(start);
    return inputStream;
  }

  public void delete(final String bucket, final String path) throws IOException
  {
    storage.get().objects().delete(bucket, path).execute();
  }

  public boolean exists(final String bucket, final String path)
  {
    try {
      return storage.get().objects().get(bucket, path).executeUsingHead().isSuccessStatusCode();
    }
    catch (Exception e) {
      return false;
    }
  }
   
  public long size(final String bucket, final String path) throws IOException
  {
    return storage.get().objects().get(bucket, path).execute().getSize().longValue();
  }

  public String version(final String bucket, final String path) throws IOException
  {
    return storage.get().objects().get(bucket, path).execute().getEtag();
  }

  public Storage.Objects.List list(final String bucket) throws IOException
  {
    return storage.get().objects().list(bucket);
  }
}
