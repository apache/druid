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

package org.apache.druid.indexing.seekablestream;

import com.google.common.base.Throwables;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.systemfield.SystemFieldDecoratorFactory;
import org.apache.druid.indexing.overlord.sampler.SamplerException;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link org.apache.druid.data.input.InputSource} wrapping {@link RecordSupplier}. It will fetch data via
 * RecordSupplier and convert it into {@link ByteEntity}. See {@link #createEntityIterator}.
 */
public class RecordSupplierInputSource<PartitionIdType, SequenceOffsetType, RecordType extends ByteEntity> extends AbstractInputSource
{
  private static final Logger LOG = new Logger(RecordSupplierInputSource.class);

  private final String topic;
  private final RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> recordSupplier;
  private final boolean useEarliestOffset;

  /**
   * Maximum amount of time in which the entity iterator will return results. If null, no timeout is applied.
   */
  private final Integer iteratorTimeoutMs;

  public RecordSupplierInputSource(
      String topic,
      RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> recordSupplier,
      boolean useEarliestOffset,
      Integer iteratorTimeoutMs
  )
  {
    this.topic = topic;
    this.recordSupplier = recordSupplier;
    this.useEarliestOffset = useEarliestOffset;
    this.iteratorTimeoutMs = iteratorTimeoutMs;

    assignAndSeek(recordSupplier);
  }

  private void assignAndSeek(RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> recordSupplier)
  {
    try {
      final Set<StreamPartition<PartitionIdType>> partitions = recordSupplier
          .getPartitionIds(topic)
          .stream()
          .map(partitionId -> StreamPartition.of(topic, partitionId))
          .collect(Collectors.toSet());

      recordSupplier.assign(partitions);

      if (useEarliestOffset) {
        recordSupplier.seekToEarliest(partitions);
      } else {
        recordSupplier.seekToLatest(partitions);
      }
    }
    catch (Exception e) {
      throw new SamplerException(
          e,
          "Exception while seeking to the [%s] offset of partitions in topic [%s]: %s",
          useEarliestOffset ? "earliest" : "latest",
          topic,
          Throwables.getRootCause(e).getMessage()
      );
    }
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public boolean needsFormat()
  {
    return true;
  }

  @Override
  protected InputSourceReader formattableReader(
      InputRowSchema inputRowSchema,
      InputFormat inputFormat,
      @Nullable File temporaryDirectory
  )
  {
    InputFormat format = JsonInputFormat.withLineSplittable(inputFormat, false);
    return new InputEntityIteratingReader(
        inputRowSchema,
        format,
        createEntityIterator(),
        SystemFieldDecoratorFactory.NONE,
        temporaryDirectory
    );
  }

  /**
   * Returns an iterator converting a RecordSupplier into an iterator of ByteEntity. Note that the
   * returned iterator will be blocked until the RecordSupplier gives any data.
   */
  CloseableIterator<InputEntity> createEntityIterator()
  {
    return new CloseableIterator<InputEntity>()
    {
      private Iterator<OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType, RecordType>> recordIterator;
      private Iterator<? extends ByteEntity> bytesIterator;
      private volatile boolean closed;
      private final long createTime = System.currentTimeMillis();
      private final Long terminationTime = iteratorTimeoutMs != null ? createTime + iteratorTimeoutMs : null;

      private void waitNextIteratorIfNecessary()
      {
        while (!closed && (bytesIterator == null || !bytesIterator.hasNext())) {
          while (!closed && (recordIterator == null || !recordIterator.hasNext())) {
            if (terminationTime != null && System.currentTimeMillis() > terminationTime) {
              LOG.info(
                  "Configured sampler timeout [%s] has been exceeded, returning without a bytesIterator.",
                  iteratorTimeoutMs
              );
              bytesIterator = null;
              return;
            }
            recordIterator = recordSupplier.poll(SeekableStreamSamplerSpec.POLL_TIMEOUT_MS).iterator();
          }

          if (!closed) {
            bytesIterator = recordIterator.next().getData().iterator();
          }
        }
      }

      @Override
      public boolean hasNext()
      {
        waitNextIteratorIfNecessary();
        return bytesIterator != null && bytesIterator.hasNext();
      }

      @Override
      public InputEntity next()
      {
        return bytesIterator.next();
      }

      @Override
      public void close()
      {
        LOG.info("Closing entity iterator.");
        closed = true;
        recordSupplier.close();
      }
    };
  }
}
