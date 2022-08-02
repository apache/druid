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
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.overlord.sampler.InputSourceSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.overlord.sampler.SamplerException;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import org.apache.druid.segment.indexing.DataSchema;

import javax.annotation.Nullable;

public abstract class SeekableStreamSamplerSpec<PartitionIdType, SequenceOffsetType, RecordType extends ByteEntity> implements SamplerSpec
{
  static final long POLL_TIMEOUT_MS = 100;

  @Nullable
  private final DataSchema dataSchema;
  private final InputSourceSampler inputSourceSampler;
  private final InputFormat inputFormat;

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
    this.inputFormat = Preconditions.checkNotNull(ioConfig.getInputFormat(), "[spec.ioConfig.inputFormat] is required");
    this.tuningConfig = ingestionSpec.getTuningConfig();
    this.samplerConfig = samplerConfig == null ? SamplerConfig.empty() : samplerConfig;
    this.inputSourceSampler = inputSourceSampler;
  }

  @Override
  public SamplerResponse sample()
  {
    final InputSource inputSource;

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
        ioConfig.isUseEarliestSequenceNumber()
    );

    return inputSourceSampler.sample(inputSource, inputFormat, dataSchema, samplerConfig);
  }

  protected abstract RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> createRecordSupplier();

}
