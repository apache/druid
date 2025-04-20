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

package org.apache.druid.sql.calcite.util.datasets;

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.RowSignature.Builder;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;

import java.io.File;

public class InputSourceBasedTestDataset implements TestDataSet
{
  protected final InputSource inputSource;
  protected final InputFormat inputFormat;
  protected final DataSchema dataSchema;

  public InputSourceBasedTestDataset(DataSchema dataSchema, InputFormat inputFormat, InputSource inputSource)
  {
    this.inputSource = inputSource;
    this.inputFormat = inputFormat;
    this.dataSchema = dataSchema;
  }

  @Override
  public String getName()
  {
    return getDataSchema().getDataSource();
  }

  @Override
  public final DataSegment makeSegment(final QueryableIndex index)
  {
    DataSegment segment = DataSegment.builder()
        .dataSource(getName())
        .interval(index.getDataInterval())
        .version("1")
        .shardSpec(new LinearShardSpec(0))
        .size(0)
        .build();
    return segment;
  }

  @Override
  public final QueryableIndex makeIndex(File tmpDir)
  {
    return IndexBuilder
        .create()
        .inputTmpDir(tmpDir)
        .tmpDir(tmpDir)
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(getIndexSchema())
        .inputSource(getInputSource())
        .inputFormat(getInputFormat())
        .buildMMappedIndex();
  }

  public IncrementalIndexSchema getIndexSchema()
  {
    return new IncrementalIndexSchema.Builder()
        .withTimestampSpec(getDataSchema().getTimestampSpec())
        .withMetrics(getDataSchema().getAggregators())
        .withDimensionsSpec(getDataSchema().getDimensionsSpec())
        .withRollup(getDataSchema().getGranularitySpec().isRollup())
        .withQueryGranularity(getDataSchema().getGranularitySpec().getQueryGranularity())
        .build();
  }

  public RowSignature getInputRowSignature()
  {
    Builder rsBuilder = RowSignature.builder();
    for (DimensionSchema dimensionSchema : getDataSchema().getDimensionsSpec().getDimensions()) {
      rsBuilder.add(dimensionSchema.getName(), dimensionSchema.getColumnType());
    }
    return rsBuilder.build();
  }

  protected DataSchema getDataSchema()
  {
    return dataSchema;
  }

  protected InputSource getInputSource()
  {
    return inputSource;
  }

  protected InputFormat getInputFormat()
  {
    return inputFormat;
  }
}
