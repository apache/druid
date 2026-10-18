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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

final class ShareGroupRawRecord
{
  static final class RawHeader
  {
    private final String key;
    @Nullable
    private final byte[] value;

    RawHeader(String key, @Nullable byte[] value)
    {
      this.key = Objects.requireNonNull(key, "key");
      this.value = copy(value);
    }

    String getKey()
    {
      return key;
    }

    @Nullable
    byte[] getValue()
    {
      return copy(value);
    }

    long estimatedSize()
    {
      return key.getBytes(StandardCharsets.UTF_8).length + (value == null ? 0 : value.length);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final RawHeader rawHeader = (RawHeader) o;
      return key.equals(rawHeader.key) && Arrays.equals(value, rawHeader.value);
    }

    @Override
    public int hashCode()
    {
      return 31 * key.hashCode() + Arrays.hashCode(value);
    }
  }

  private final long offset;
  private final long timestamp;
  private final TimestampType timestampType;
  private final int serializedKeySize;
  private final int serializedValueSize;
  @Nullable
  private final byte[] key;
  @Nullable
  private final byte[] value;
  private final List<RawHeader> headers;
  private final Optional<Integer> leaderEpoch;
  private final Optional<Short> deliveryCount;

  ShareGroupRawRecord(
      long offset,
      long timestamp,
      TimestampType timestampType,
      int serializedKeySize,
      int serializedValueSize,
      @Nullable byte[] key,
      @Nullable byte[] value,
      List<RawHeader> headers,
      Optional<Integer> leaderEpoch,
      Optional<Short> deliveryCount
  )
  {
    this.offset = offset;
    this.timestamp = timestamp;
    this.timestampType = Objects.requireNonNull(timestampType, "timestampType");
    this.serializedKeySize = serializedKeySize;
    this.serializedValueSize = serializedValueSize;
    this.key = copy(key);
    this.value = copy(value);
    this.headers = List.copyOf(headers);
    this.leaderEpoch = Objects.requireNonNull(leaderEpoch, "leaderEpoch");
    this.deliveryCount = Objects.requireNonNull(deliveryCount, "deliveryCount");
  }

  static ShareGroupRawRecord from(ConsumerRecord<byte[], byte[]> record)
  {
    final List<RawHeader> headers = new ArrayList<>();
    for (Header header : record.headers()) {
      headers.add(new RawHeader(header.key(), header.value()));
    }
    return new ShareGroupRawRecord(
        record.offset(),
        record.timestamp(),
        record.timestampType(),
        record.serializedKeySize(),
        record.serializedValueSize(),
        record.key(),
        record.value(),
        headers,
        record.leaderEpoch(),
        record.deliveryCount()
    );
  }

  ConsumerRecord<byte[], byte[]> toConsumerRecord(String topic, int partition)
  {
    final RecordHeaders recordHeaders = new RecordHeaders();
    for (RawHeader header : headers) {
      recordHeaders.add(new RecordHeader(header.key, header.getValue()));
    }
    return new ConsumerRecord<>(
        topic,
        partition,
        offset,
        timestamp,
        timestampType,
        serializedKeySize,
        serializedValueSize,
        copy(key),
        copy(value),
        recordHeaders,
        leaderEpoch,
        deliveryCount
    );
  }

  long getOffset()
  {
    return offset;
  }

  long getTimestamp()
  {
    return timestamp;
  }

  TimestampType getTimestampType()
  {
    return timestampType;
  }

  int getSerializedKeySize()
  {
    return serializedKeySize;
  }

  int getSerializedValueSize()
  {
    return serializedValueSize;
  }

  @Nullable
  byte[] getKey()
  {
    return copy(key);
  }

  @Nullable
  byte[] getValue()
  {
    return copy(value);
  }

  List<RawHeader> getHeaders()
  {
    return headers;
  }

  Optional<Integer> getLeaderEpoch()
  {
    return leaderEpoch;
  }

  Optional<Short> getDeliveryCount()
  {
    return deliveryCount;
  }

  long estimatedSize()
  {
    long size = Long.BYTES * 2L + Integer.BYTES * 4L + 2L;
    size += key == null ? 0 : key.length;
    size += value == null ? 0 : value.length;
    for (RawHeader header : headers) {
      size += header.estimatedSize() + Integer.BYTES * 2L;
    }
    return size;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ShareGroupRawRecord that = (ShareGroupRawRecord) o;
    return offset == that.offset
           && timestamp == that.timestamp
           && serializedKeySize == that.serializedKeySize
           && serializedValueSize == that.serializedValueSize
           && timestampType == that.timestampType
           && Arrays.equals(key, that.key)
           && Arrays.equals(value, that.value)
           && headers.equals(that.headers)
           && leaderEpoch.equals(that.leaderEpoch)
           && deliveryCount.equals(that.deliveryCount);
  }

  @Override
  public int hashCode()
  {
    int result = Objects.hash(
        offset,
        timestamp,
        timestampType,
        serializedKeySize,
        serializedValueSize,
        headers,
        leaderEpoch,
        deliveryCount
    );
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Nullable
  private static byte[] copy(@Nullable byte[] value)
  {
    return value == null ? null : Arrays.copyOf(value, value.length);
  }
}
