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

package org.apache.druid.indexing.kafka;

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.storage.StorageConnector;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

final class ShareGroupBatchStore
{
  static final class StoredBatch
  {
    private final String path;
    private final String sha256;
    private final long size;
    private final ShareGroupRawBatch batch;

    private StoredBatch(String path, String sha256, long size, ShareGroupRawBatch batch)
    {
      this.path = path;
      this.sha256 = sha256;
      this.size = size;
      this.batch = batch;
    }

    String getPath()
    {
      return path;
    }

    String getSha256()
    {
      return sha256;
    }

    long getSize()
    {
      return size;
    }

    ShareGroupRawBatch getBatch()
    {
      return batch;
    }
  }

  private static final class CountingOutputStream extends FilterOutputStream
  {
    private long count;

    private CountingOutputStream(OutputStream outputStream)
    {
      super(outputStream);
    }

    @Override
    public void write(int value) throws IOException
    {
      out.write(value);
      count++;
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException
    {
      out.write(bytes, offset, length);
      count += length;
    }

    private long getCount()
    {
      return count;
    }
  }

  private final StorageConnector storageConnector;
  private final String taskId;
  private final ShareGroupRawBatchWriter writer = new ShareGroupRawBatchWriter();
  private final ShareGroupRawBatchReader reader = new ShareGroupRawBatchReader();

  ShareGroupBatchStore(StorageConnector storageConnector, String taskId)
  {
    this.storageConnector = Preconditions.checkNotNull(storageConnector, "storageConnector");
    this.taskId = Preconditions.checkNotNull(taskId, "taskId");
  }

  StoredBatch store(
      List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records
  ) throws IOException
  {
    return store(ShareGroupRawBatch.fromOrderedRecords(records));
  }

  StoredBatch store(ShareGroupRawBatch batch) throws IOException
  {
    final String path = objectPath(batch);
    final MessageDigest writeDigest = ShareGroupRawBatchWriter.newDigest();
    final CountingOutputStream countingOutput;
    try (OutputStream storageOutput = storageConnector.write(path)) {
      countingOutput = new CountingOutputStream(storageOutput);
      writer.write(batch, new DigestOutputStream(countingOutput, writeDigest));
    }
    catch (IOException | RuntimeException e) {
      deleteQuietly(path, e);
      throw e;
    }

    final byte[] expectedDigest = writeDigest.digest();
    try {
      if (!storageConnector.pathExists(path)) {
        throw new IOException("Stored share-group raw batch does not exist at " + path);
      }
      final MessageDigest readDigest = ShareGroupRawBatchWriter.newDigest();
      final ShareGroupRawBatch verifiedBatch;
      try (InputStream storageInput = storageConnector.read(path);
           DigestInputStream digestInput = new DigestInputStream(storageInput, readDigest)) {
        verifiedBatch = reader.read(digestInput, batch.getTopic(), batch.getPartition());
      }
      if (!batch.equals(verifiedBatch) || !Arrays.equals(expectedDigest, readDigest.digest())) {
        throw new IOException("Stored share-group raw batch verification failed at " + path);
      }
      return new StoredBatch(path, HexFormat.of().formatHex(expectedDigest), countingOutput.getCount(), batch);
    }
    catch (IOException | RuntimeException e) {
      deleteQuietly(path, e);
      throw e;
    }
  }

  ShareGroupRawBatch read(StoredBatch storedBatch) throws IOException
  {
    return read(
        storedBatch.path,
        storedBatch.sha256,
        storedBatch.batch.getTopic(),
        storedBatch.batch.getPartition()
    );
  }

  ShareGroupRawBatch read(
      String path,
      String sha256,
      String expectedTopic,
      int expectedPartition
  ) throws IOException
  {
    final MessageDigest digest = ShareGroupRawBatchWriter.newDigest();
    final ShareGroupRawBatch batch;
    try (InputStream storageInput = storageConnector.read(path);
         DigestInputStream digestInput = new DigestInputStream(storageInput, digest)) {
      batch = reader.read(
          digestInput,
          expectedTopic,
          expectedPartition
      );
    }
    if (!sha256.equals(HexFormat.of().formatHex(digest.digest()))) {
      throw new IOException("Stored share-group raw batch object checksum mismatch at " + path);
    }
    return batch;
  }

  void delete(StoredBatch storedBatch) throws IOException
  {
    if (storageConnector.pathExists(storedBatch.path)) {
      storageConnector.deleteFile(storedBatch.path);
    }
  }

  private String objectPath(ShareGroupRawBatch batch)
  {
    return "share-inbox/v1/"
           + encode(batch.getTopic()) + '/'
           + batch.getPartition() + '/'
           + encode(taskId) + '/'
           + batch.getFirstOffset() + '-' + batch.getLastOffset() + '-'
           + UUID.randomUUID() + ".sgb";
  }

  private void deleteQuietly(String path, Exception original)
  {
    try {
      if (storageConnector.pathExists(path)) {
        storageConnector.deleteFile(path);
      }
    }
    catch (Exception cleanupFailure) {
      original.addSuppressed(cleanupFailure);
    }
  }

  private static String encode(String value)
  {
    return Base64.getUrlEncoder()
                 .withoutPadding()
                 .encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }
}
