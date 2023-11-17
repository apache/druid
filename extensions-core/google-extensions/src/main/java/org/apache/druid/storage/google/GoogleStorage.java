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
import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

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

  public static class GoogleStorageObjectMetadata
  {
    final String bucket;
    final String name;
    final Long size;
    Long lastUpdateTime;

    public GoogleStorageObjectMetadata(final String bucket, final String name, final Long size, final Long lastUpdateTime)
    {
      this.bucket = bucket;
      this.name = name;
      this.size = size;
      this.lastUpdateTime = lastUpdateTime;
    }

    public void setLastUpdateTime(Long lastUpdateTime)
    {
      this.lastUpdateTime = lastUpdateTime;
    }


    public String getBucket()
    {
      return bucket;
    }

    public String getName()
    {
      return name;
    }

    public Long getSize()
    {
      return size;
    }

    public Long getLastUpdateTime()
    {
      return lastUpdateTime;
    }
  }

  public static class GoogleStorageObjectPage
  {
    final List<GoogleStorage.GoogleStorageObjectMetadata> objectList;

    @Nullable
    final String nextPageToken;

    public GoogleStorageObjectPage(
        List<GoogleStorage.GoogleStorageObjectMetadata> objectList,
        String nextPageToken
    )
    {
      this.objectList = objectList;
      this.nextPageToken = nextPageToken;
    }

    public List<GoogleStorage.GoogleStorageObjectMetadata> getObjectList()
    {
      return objectList;
    }

    @Nullable
    public String getNextPageToken()
    {
      return nextPageToken;
    }
  }

  public GoogleStorage(final Supplier<Storage> storage)
  {
    this.storage = storage;
  }

  public void insert(final String bucket, final String path, AbstractInputStreamContent mediaContent) throws IOException
  {
    storage.get().createFrom(getBlobInfo(bucket, path), mediaContent.getInputStream());
  }

  public InputStream getInputStream(final String bucket, final String path) throws IOException
  {
    return getInputStream(bucket, path, 0, null);
  }

  public InputStream getInputStream(final String bucket, final String path, long start) throws IOException
  {
    return getInputStream(bucket, path, start, null);
  }

  public InputStream getInputStream(final String bucket, final String path, long start, @Nullable Long length) throws IOException
  {
    try (ReadChannel reader = storage.get().reader(bucket, path)) {
      reader.seek(start);
      if (length != null) {
        reader.limit(start + length - 1);
      }
      return Channels.newInputStream(reader);
    }
  }

  public OutputStream getObjectOutputStream(
      final String bucket,
      final String path
  )
  {
    WriteChannel writer = storage.get().writer(getBlobInfo(bucket, path));
    return Channels.newOutputStream(writer);
  }

  public GoogleStorage.GoogleStorageObjectMetadata getMetadata(
      final String bucket,
      final String path
  )
  {
    Blob blob = storage.get().get(bucket, path, Storage.BlobGetOption.fields(Storage.BlobField.values()));
    return new GoogleStorage.GoogleStorageObjectMetadata(
        blob.getBucket(),
        blob.getName(),
        blob.getSize(),
        blob.getUpdateTimeOffsetDateTime()
            .toEpochSecond()
    );
  }

  public void delete(final String bucket, final String path) throws IOException
  {
    storage.get().delete(bucket, path);
  }
  public boolean exists(final String bucket, final String path)
  {

    Blob blob = storage.get().get(bucket, path);
    return blob != null;
  }

  public long size(final String bucket, final String path) throws IOException
  {
    Blob blob = storage.get().get(bucket, path, Storage.BlobGetOption.fields(Storage.BlobField.SIZE));
    return blob.getSize();
  }

  public String version(final String bucket, final String path) throws IOException
  {
    Blob blob = storage.get().get(bucket, path, Storage.BlobGetOption.fields(Storage.BlobField.GENERATION));
    return blob.getGeneratedId();
  }

  public GoogleStorage.GoogleStorageObjectPage list(
      final String bucket,
      @Nullable final String prefix,
      @Nullable final Long pageSize,
      @Nullable final String pageToken
  ) throws IOException
  {
    List<Storage.BlobListOption> options = new ArrayList<>();

    if (prefix != null) {
      options.add(Storage.BlobListOption.prefix(prefix));
    }

    if (pageSize != null) {
      options.add(Storage.BlobListOption.pageSize(pageSize));
    }

    if (pageToken != null) {
      options.add(Storage.BlobListOption.pageToken(pageToken));
    }

    Page<Blob> blobPage = storage.get().list(bucket, options.toArray(new Storage.BlobListOption[0]));

    List<GoogleStorage.GoogleStorageObjectMetadata> googleStorageObjectMetadataList = blobPage.streamValues()
                                                                                              .map(blob -> new GoogleStorage.GoogleStorageObjectMetadata(
                                                                                                  blob.getBucket(),
                                                                                                  blob.getName(),
                                                                                                  blob.getSize(),
                                                                                                  blob.getUpdateTimeOffsetDateTime()
                                                                                                      .toEpochSecond()
                                                                                              ))
                                                                                              .collect(Collectors.toList());

    return new GoogleStorage.GoogleStorageObjectPage(googleStorageObjectMetadataList, blobPage.getNextPageToken());

  }
  public void batchDelete(final String bucket, final Iterable<String> paths){
    storage.get().delete(Iterables.transform(paths, input -> BlobId.of(bucket, input)));
  }

  private BlobInfo getBlobInfo(final String bucket, final String path)
  {
    BlobId blobId = BlobId.of(bucket, path);
    return BlobInfo.newBuilder(blobId).build();

  }




}
