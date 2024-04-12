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

package org.apache.druid.msq.statistics.serde;

import org.apache.druid.frame.key.RowKey;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.KeyCollectorSnapshot;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Handles the serialization and deserialization of {@link ClusterByStatisticsSnapshot}, into a byte array.
 */
public class ClusterByStatisticsSnapshotSerde
{
  /**
   * Deserializes the {@link ClusterByStatisticsSnapshot} and writes it to the {@link OutputStream}.
   * <br>
   * Format:
   * - 1 byte : Header byte
   * - 4 bytes: Number of buckets
   * - 4 bytes: Number of entries in {@link ClusterByStatisticsSnapshot#getHasMultipleValues()}
   * - 4 * number of multivalue bucket bytes: List of integers
   * - number of buckets * buckets, serailized by {@link #serializeBucket(OutputStream, ClusterByStatisticsSnapshot.Bucket)}
   */
  public static void serialize(OutputStream outputStream, ClusterByStatisticsSnapshot snapshot) throws IOException
  {
    final Map<Long, ClusterByStatisticsSnapshot.Bucket> buckets = snapshot.getBuckets();
    final Set<Integer> multipleValueBuckets = snapshot.getHasMultipleValues();

    // Write a header byte, to be used to contain any metadata in the future.
    outputStream.write(0x0);

    writeIntToStream(outputStream, buckets.size());
    writeIntToStream(outputStream, multipleValueBuckets.size());

    for (Integer multiValueBucket : multipleValueBuckets) {
      writeIntToStream(outputStream, multiValueBucket); // TODO: batch into a single buffer
    }

    for (Map.Entry<Long, ClusterByStatisticsSnapshot.Bucket> entry : buckets.entrySet()) {
      writeLongToStream(outputStream, entry.getKey()); // TODO: move this to inside the bucket?
      serializeBucket(outputStream, entry.getValue());
    }
  }

  private static final int HEADER_OFFSET = 0;
  private static final int BUCKET_COUNT_OFFSET = HEADER_OFFSET + Byte.BYTES;
  private static final int MV_SET_SIZE_OFFSET = BUCKET_COUNT_OFFSET + Integer.BYTES;
  private static final int MV_VALUES_OFFSET = MV_SET_SIZE_OFFSET + Integer.BYTES;

  private static final int TIMECHUNK_OFFSET = 0;
  private static final int BUCKET_SIZE_OFFSET = TIMECHUNK_OFFSET + Long.BYTES;
  private static final int BUCKET_OFFSET = BUCKET_SIZE_OFFSET + Integer.BYTES;

  public static ClusterByStatisticsSnapshot deserialize(ByteBuffer byteBuffer)
  {
    int position = byteBuffer.position();

    final int bucketCount = byteBuffer.getInt(position + BUCKET_COUNT_OFFSET);
    final int mvSetSize = byteBuffer.getInt(position + MV_SET_SIZE_OFFSET);

    final Set<Integer> hasMultiValues = new HashSet<>();
    for (int i = 0; i < mvSetSize; i++) {
      // TODO: use better read
      hasMultiValues.add(byteBuffer.getInt(position + MV_VALUES_OFFSET + Integer.BYTES * i));
    }

    final Map<Long, ClusterByStatisticsSnapshot.Bucket> buckets = new HashMap<>();

    // Move the buffer position
    int nextBucket = position + MV_VALUES_OFFSET + Integer.BYTES * mvSetSize;

    for (int bucketNo = 0; bucketNo < bucketCount; bucketNo++) {
      position = byteBuffer.position(nextBucket).position();

      final long timeChunk = byteBuffer.getLong(position + TIMECHUNK_OFFSET);
      final int snapshotSize = byteBuffer.getInt(position + BUCKET_SIZE_OFFSET);

      final ByteBuffer duplicate = (ByteBuffer) byteBuffer.duplicate()
                                                          .order(byteBuffer.order())
                                                          .position(position + BUCKET_OFFSET)
                                                          .limit(position + BUCKET_OFFSET + snapshotSize);

      ClusterByStatisticsSnapshot.Bucket bucket = deserializeBucket(duplicate);
      buckets.put(timeChunk, bucket);

      nextBucket = position + BUCKET_OFFSET + snapshotSize;
    }

    return new ClusterByStatisticsSnapshot(buckets, hasMultiValues);
  }

  /**
   * Format:
   * - 8 bytes: bytesRetained
   * - 4 bytes: keyArray length
   * - 4 bytes: snapshot length
   * - keyArray length bytes: serialized key array
   * - snapshot length bytes: serialized snapshot
   */
  static void serializeBucket(OutputStream outputStream, ClusterByStatisticsSnapshot.Bucket bucket) throws IOException
  {
    final byte[] bucketKeyArray = bucket.getBucketKey().array();
    final double bytesRetained = bucket.getBytesRetained();

    final KeyCollectorSnapshot snapshot = bucket.getKeyCollectorSnapshot();
    final byte[] serializedSnapshot = snapshot.getSerializer().serialize(snapshot);

    final int length = Double.BYTES + 2 * Integer.BYTES + bucketKeyArray.length + serializedSnapshot.length;

    outputStream.write(
        ByteBuffer.allocate(Integer.BYTES + length)
                  .putInt(length)
                  .putDouble(bytesRetained)
                  .putInt(bucketKeyArray.length)
                  .putInt(serializedSnapshot.length)
                  .put(bucketKeyArray)
                  .put(serializedSnapshot)
                  .array()
    );
  }

  private static final int BYTES_RETAINED_OFFSET = 0;
  private static final int KEY_LENGTH_OFFSET = BYTES_RETAINED_OFFSET + Double.BYTES;
  private static final int SNAPSHOT_LENGTH_OFFSET = KEY_LENGTH_OFFSET + Integer.BYTES;
  private static final int KEY_OFFSET = SNAPSHOT_LENGTH_OFFSET + Integer.BYTES;

  static ClusterByStatisticsSnapshot.Bucket deserializeBucket(ByteBuffer byteBuffer)
  {
    int position = byteBuffer.position();
    final double bytesRetained = byteBuffer.getDouble(position + BYTES_RETAINED_OFFSET);
    final int keyLength = byteBuffer.getInt(position + KEY_LENGTH_OFFSET);
    final int snapshotLength = byteBuffer.getInt(position + SNAPSHOT_LENGTH_OFFSET);

    final ByteBuffer keyBuffer = (ByteBuffer) byteBuffer.duplicate()
                                                        .order(byteBuffer.order())
                                                        .position(position + KEY_OFFSET)
                                                        .limit(position + KEY_OFFSET + keyLength);

    final byte[] byteKey = new byte[keyLength];
    keyBuffer.get(byteKey);

    final int snapshotOffset = position + KEY_OFFSET + keyLength;
    final ByteBuffer snapshotBuffer = (ByteBuffer) byteBuffer.duplicate()
                                                             .order(byteBuffer.order())
                                                             .position(snapshotOffset)
                                                             .limit(snapshotOffset + snapshotLength);

    return new ClusterByStatisticsSnapshot.Bucket(
        RowKey.wrap(byteKey),
        KeyCollectorSnapshotDeserializer.deserialize(snapshotBuffer),
        bytesRetained
    );
  }

  private static void writeIntToStream(OutputStream outputStream, int integerToWrite) throws IOException
  {
    outputStream.write(ByteBuffer.allocate(Integer.BYTES).putInt(integerToWrite).array());
  }

  private static void writeLongToStream(OutputStream outputStream, long longToWrite) throws IOException
  {
    outputStream.write(ByteBuffer.allocate(Long.BYTES).putLong(longToWrite).array());
  }
}
