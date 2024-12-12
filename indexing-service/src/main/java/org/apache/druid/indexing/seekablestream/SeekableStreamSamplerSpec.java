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
import com.google.common.base.Throwables;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.client.indexing.SamplerSpec;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.indexing.overlord.sampler.InputSourceSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.overlord.sampler.SamplerException;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.indexing.DataSchema;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public abstract class SeekableStreamSamplerSpec<PartitionIdType, SequenceOffsetType, RecordType extends ByteEntity> implements SamplerSpec
{
  static final long POLL_TIMEOUT_MS = 100;

  @Nullable
  private final DataSchema dataSchema;
  private final InputSourceSampler inputSourceSampler;

  protected final SeekableStreamSupervisorIOConfig ioConfig;
  @Nullable
  protected final SeekableStreamSupervisorTuningConfig tuningConfig;
  protected final SamplerConfig samplerConfig;

  public SeekableStreamSamplerSpec(
      final SeekableStreamSupervisorSpec ingestionSpec,
      @Nullable final SamplerConfig samplerConfig,
      final InputSourceSampler inputSourceSampler
  )
  {
    this.dataSchema = Preconditions.checkNotNull(ingestionSpec, "[spec] is required").getDataSchema();
    this.ioConfig = Preconditions.checkNotNull(ingestionSpec.getIoConfig(), "[spec.ioConfig] is required");
    this.tuningConfig = ingestionSpec.getTuningConfig();
    this.samplerConfig = samplerConfig == null ? SamplerConfig.empty() : samplerConfig;
    this.inputSourceSampler = inputSourceSampler;
  }

  @Override
  public SamplerResponse sample()
  {
    final InputSource inputSource;
    final InputFormat inputFormat;
    if (dataSchema.getParser() != null) {
      inputSource = new SeekableStreamSamplerInputSource(dataSchema.getParser());
      inputFormat = null;
    } else {
      RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> recordSupplier;

      try {
        recordSupplier = createRecordSupplier();
      }
      catch (Exception e) {
        throw new SamplerException(e, "Unable to create RecordSupplier: %s", Throwables.getRootCause(e).getMessage());
      }

      inputSource = new RecordSupplierInputSource<>(
          ioConfig.getStream(),
          recordSupplier,
          ioConfig.isUseEarliestSequenceNumber(),
          samplerConfig.getTimeoutMs() <= 0 ? null : samplerConfig.getTimeoutMs()
      );
      inputFormat = Preconditions.checkNotNull(
          ioConfig.getInputFormat(),
          "[spec.ioConfig.inputFormat] is required"
      );
    }

    return inputSourceSampler.sample(inputSource, inputFormat, dataSchema, samplerConfig);
  }

  protected abstract RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> createRecordSupplier();

  private class SeekableStreamSamplerInputSource extends AbstractInputSource implements SplittableInputSource
  {
    private final InputRowParser parser;

    public SeekableStreamSamplerInputSource(InputRowParser parser)
    {
      this.parser = parser;
    }

    public InputRowParser getParser()
    {
      return parser;
    }

    @Override
    public boolean isSplittable()
    {
      return false;
    }

    @Override
    public Stream<InputSplit> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public SplittableInputSource withSplit(InputSplit split)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean needsFormat()
    {
      return false;
    }

    @Override
    protected InputSourceReader fixedFormatReader(InputRowSchema inputRowSchema, @Nullable File temporaryDirectory)
    {
      return new SeekableStreamSamplerInputSourceReader(parser);
    }
  }

  private class SeekableStreamSamplerInputSourceReader implements InputSourceReader
  {
    private final InputRowParser parser;
    private final CloseableIterator<InputEntity> entityIterator;

    public SeekableStreamSamplerInputSourceReader(InputRowParser parser)
    {
      this.parser = parser;
      if (parser instanceof StringInputRowParser) {
        ((StringInputRowParser) parser).startFileFromBeginning();
      }

      RecordSupplierInputSource<PartitionIdType, SequenceOffsetType, RecordType> inputSource = new RecordSupplierInputSource<>(
          ioConfig.getStream(),
          createRecordSupplier(),
          ioConfig.isUseEarliestSequenceNumber(),
          samplerConfig.getTimeoutMs() <= 0 ? null : samplerConfig.getTimeoutMs()
      );
      this.entityIterator = inputSource.createEntityIterator();
    }

    @Override
    public CloseableIterator<InputRow> read()
    {
      return new CloseableIterator<InputRow>()
      {

        @Override
        public boolean hasNext()
        {
          return entityIterator.hasNext();
        }

        @Override
        public InputRow next()
        {
          throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException
        {
          entityIterator.close();
        }
      };
    }

    @Override
    public CloseableIterator<InputRow> read(InputStats inputStats)
    {
      return null;
    }

    @Override
    public CloseableIterator<InputRowListPlusRawValues> sample()
    {
      return new CloseableIterator<InputRowListPlusRawValues>()
      {
        @Override
        public boolean hasNext()
        {
          return entityIterator.hasNext();
        }

        @Override
        public InputRowListPlusRawValues next()
        {
          // We need to modify the position of the buffer, so duplicate it.
          final ByteBuffer bb = ((ByteEntity) entityIterator.next()).getBuffer().duplicate();

          final Map<String, Object> rawColumns;
          try {
            if (parser instanceof StringInputRowParser) {
              rawColumns = ((StringInputRowParser) parser).buildStringKeyMap(bb);
            } else {
              rawColumns = null;
            }
          }
          catch (ParseException e) {
            return InputRowListPlusRawValues.of(null, e);
          }

          try {
            bb.position(0);
            final List<InputRow> rows = parser.parseBatch(bb);
            return InputRowListPlusRawValues.of(rows.isEmpty() ? null : rows, rawColumns);
          }
          catch (ParseException e) {
            return InputRowListPlusRawValues.of(rawColumns, e);
          }
        }

        @Override
        public void close() throws IOException
        {
          entityIterator.close();
        }
      };
    }
  }
}
