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

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowPlusRaw;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.indexing.overlord.sampler.FirehoseSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.overlord.sampler.SamplerException;
import org.apache.druid.indexing.overlord.sampler.SamplerResponse;
import org.apache.druid.indexing.overlord.sampler.SamplerSpec;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.indexing.DataSchema;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class SeekableStreamSamplerSpec<PartitionIdType, SequenceOffsetType> implements SamplerSpec
{
  private static final int POLL_TIMEOUT_MS = 100;

  private final DataSchema dataSchema;
  private final FirehoseSampler firehoseSampler;

  protected final SeekableStreamSupervisorIOConfig ioConfig;
  protected final SeekableStreamSupervisorTuningConfig tuningConfig;
  protected final SamplerConfig samplerConfig;

  public SeekableStreamSamplerSpec(
      final SeekableStreamSupervisorSpec ingestionSpec,
      final SamplerConfig samplerConfig,
      final FirehoseSampler firehoseSampler
  )
  {
    this.dataSchema = Preconditions.checkNotNull(ingestionSpec, "[spec] is required").getDataSchema();
    this.ioConfig = Preconditions.checkNotNull(ingestionSpec.getIoConfig(), "[spec.ioConfig] is required");
    this.tuningConfig = ingestionSpec.getTuningConfig();
    this.samplerConfig = samplerConfig;
    this.firehoseSampler = firehoseSampler;
  }

  @Override
  public SamplerResponse sample()
  {
    return firehoseSampler.sample(
        new FirehoseFactory()
        {
          @Override
          public Firehose connect(InputRowParser parser, @Nullable File temporaryDirectory)
          {
            return getFirehose(parser);
          }
        },
        dataSchema,
        samplerConfig
    );
  }

  protected abstract Firehose getFirehose(InputRowParser parser);

  protected abstract class SeekableStreamSamplerFirehose implements Firehose
  {
    private final InputRowParser parser;
    private final RecordSupplier<PartitionIdType, SequenceOffsetType> recordSupplier;

    private Iterator<OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType>> recordIterator;
    private Iterator<byte[]> recordDataIterator;

    private volatile boolean closed = false;

    protected SeekableStreamSamplerFirehose(InputRowParser parser)
    {
      this.parser = parser;

      if (parser instanceof StringInputRowParser) {
        ((StringInputRowParser) parser).startFileFromBeginning();
      }

      this.recordSupplier = getRecordSupplier();

      try {
        assignAndSeek();
      }
      catch (InterruptedException e) {
        throw new SamplerException(e, "Exception while seeking to partitions");
      }
    }

    @Override
    public boolean hasMore()
    {
      return !closed;
    }

    @Nullable
    @Override
    public InputRow nextRow()
    {
      InputRowPlusRaw row = nextRowWithRaw();
      if (row.getParseException() != null) {
        throw row.getParseException();
      }

      return row.getInputRow();
    }

    @Override
    public InputRowPlusRaw nextRowWithRaw()
    {
      if (recordDataIterator == null || !recordDataIterator.hasNext()) {
        if (recordIterator == null || !recordIterator.hasNext()) {
          recordIterator = recordSupplier.poll(POLL_TIMEOUT_MS).iterator();

          if (!recordIterator.hasNext()) {
            return InputRowPlusRaw.of((InputRow) null, null);
          }
        }

        recordDataIterator = recordIterator.next().getData().iterator();

        if (!recordDataIterator.hasNext()) {
          return InputRowPlusRaw.of((InputRow) null, null);
        }
      }

      byte[] raw = recordDataIterator.next();

      try {
        List<InputRow> rows = parser.parseBatch(ByteBuffer.wrap(raw));
        return InputRowPlusRaw.of(rows.isEmpty() ? null : rows.get(0), raw);
      }
      catch (ParseException e) {
        return InputRowPlusRaw.of(raw, e);
      }
    }

    @Override
    public void close()
    {
      if (closed) {
        return;
      }

      closed = true;
      recordSupplier.close();
    }

    private void assignAndSeek() throws InterruptedException
    {
      final Set<StreamPartition<PartitionIdType>> partitions = recordSupplier
          .getPartitionIds(ioConfig.getStream())
          .stream()
          .map(x -> StreamPartition.of(ioConfig.getStream(), x))
          .collect(Collectors.toSet());

      recordSupplier.assign(partitions);

      if (ioConfig.isUseEarliestSequenceNumber()) {
        recordSupplier.seekToEarliest(partitions);
      } else {
        recordSupplier.seekToLatest(partitions);
      }
    }

    protected abstract RecordSupplier<PartitionIdType, SequenceOffsetType> getRecordSupplier();
  }
}
