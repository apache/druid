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
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.client.indexing.SamplerSpec;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactoryToInputSourceAdaptor;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.indexing.overlord.sampler.InputSourceSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
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
      inputSource = new FirehoseFactoryToInputSourceAdaptor(
          new SeekableStreamSamplerFirehoseFactory(),
          dataSchema.getParser()
      );
      inputFormat = null;
    } else {
      inputSource = new RecordSupplierInputSource<>(
          ioConfig.getStream(),
          createRecordSupplier(),
          ioConfig.isUseEarliestSequenceNumber()
      );
      inputFormat = Preconditions.checkNotNull(
          ioConfig.getInputFormat(),
          "[spec.ioConfig.inputFormat] is required"
      );
    }

    return inputSourceSampler.sample(inputSource, inputFormat, dataSchema, samplerConfig);
  }

  protected abstract RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> createRecordSupplier();

  private class SeekableStreamSamplerFirehoseFactory implements FiniteFirehoseFactory<ByteBufferInputRowParser, Object>
  {
    @Override
    public Firehose connect(ByteBufferInputRowParser parser, @Nullable File temporaryDirectory)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Firehose connectForSampler(ByteBufferInputRowParser parser, @Nullable File temporaryDirectory)
    {
      return new SeekableStreamSamplerFirehose(parser);
    }

    @Override
    public boolean isSplittable()
    {
      return false;
    }

    @Override
    public Stream<InputSplit<Object>> getSplits(@Nullable SplitHintSpec splitHintSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getNumSplits(@Nullable SplitHintSpec splitHintSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public FiniteFirehoseFactory withSplit(InputSplit split)
    {
      throw new UnsupportedOperationException();
    }
  }

  private class SeekableStreamSamplerFirehose implements Firehose
  {
    private final InputRowParser parser;
    private final CloseableIterator<InputEntity> entityIterator;

    protected SeekableStreamSamplerFirehose(InputRowParser parser)
    {
      this.parser = parser;
      if (parser instanceof StringInputRowParser) {
        ((StringInputRowParser) parser).startFileFromBeginning();
      }

      RecordSupplierInputSource<PartitionIdType, SequenceOffsetType, RecordType> inputSource = new RecordSupplierInputSource<>(
          ioConfig.getStream(),
          createRecordSupplier(),
          ioConfig.isUseEarliestSequenceNumber()
      );
      this.entityIterator = inputSource.createEntityIterator();
    }

    @Override
    public boolean hasMore()
    {
      return entityIterator.hasNext();
    }

    @Override
    public InputRow nextRow()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputRowListPlusRawValues nextRowWithRaw()
    {
      final ByteBuffer bb = ((ByteEntity) entityIterator.next()).getBuffer();

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
  }
}
