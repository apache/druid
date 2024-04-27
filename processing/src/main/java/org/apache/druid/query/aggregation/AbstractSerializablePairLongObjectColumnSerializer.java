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

package org.apache.druid.query.aggregation;

import com.google.common.base.Preconditions;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.serde.cell.ByteBufferProvider;
import org.apache.druid.segment.serde.cell.StagedSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * valid call sequence
 * <p>
 * open()+serialize()*(getSerializedSize()|writeTo())*
 * <p>
 * getSerializedSize() / writeTo() effectively function as a close call, but each may be called multiple times and has
 * no effect on one another.
 */
@SuppressWarnings("NotNullFieldNotInitialized")
public abstract class AbstractSerializablePairLongObjectColumnSerializer<T extends SerializablePair<Long, ?>> implements
    GenericColumnSerializer<T>
{
  public final StagedSerde<T> stagedSerde;
  final SegmentWriteOutMedium segmentWriteOutMedium;
  private final ByteBufferProvider byteBufferProvider;

  State state = State.START;
  AbstractSerializablePairLongObjectBufferStore<T> bufferStore;
  private AbstractSerializablePairLongObjectBufferStore.TransferredBuffer transferredBuffer;

  AbstractSerializablePairLongObjectColumnSerializer(
      StagedSerde<T> stagedSerde,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ByteBufferProvider byteBufferProvider
  )
  {
    this.stagedSerde = stagedSerde;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.byteBufferProvider = byteBufferProvider;
  }

  @Override
  public void serialize(ColumnValueSelector<? extends T> selector) throws IOException
  {
    Preconditions.checkState(state == State.OPEN, "serialize called in invalid state %s", state);
    T pairLongObject = selector.getObject();
    bufferStore.store(pairLongObject);
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    Preconditions.checkState(
        state != State.START,
        "getSerializedSize called in invalid state %s (must have opened at least)",
        state
    );

    transferToRowWriterIfNecessary();

    return transferredBuffer.getSerializedSize();
  }

  @Override
  public void writeTo(
      WritableByteChannel channel,
      FileSmoosher smoosher
  ) throws IOException
  {
    Preconditions.checkState(state != State.START, "writeTo called in invalid state %s", state);
    transferToRowWriterIfNecessary();
    transferredBuffer.writeTo(channel, smoosher);
  }

  private void transferToRowWriterIfNecessary() throws IOException
  {
    if (state == State.OPEN) {
      transferredBuffer = bufferStore.transferToRowWriter(byteBufferProvider, segmentWriteOutMedium);
      state = State.CLOSED;
    }
  }

  enum State
  {
    START,
    OPEN,
    CLOSED,
  }
}
