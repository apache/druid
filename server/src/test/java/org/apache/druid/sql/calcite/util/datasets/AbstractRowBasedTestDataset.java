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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.RowSignature.Builder;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;

import java.io.File;
import java.util.List;

public abstract class AbstractRowBasedTestDataset implements TestDataSet
{
  protected final String name;

  protected AbstractRowBasedTestDataset(String name)
  {
    this.name = name;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public final DataSegment makeSegment(final QueryableIndex index)
  {
    DataSegment segment = DataSegment.builder()
        .dataSource(name)
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
        .tmpDir(tmpDir)
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(getIndexSchema())
        .rows(getRows())
        .buildMMappedIndex();
  }

  public IncrementalIndexSchema getIndexSchema()
  {
    return new IncrementalIndexSchema.Builder()
        .withMetrics(getMetrics().toArray(new AggregatorFactory[0]))
        .withDimensionsSpec(getInputRowSchema().getDimensionsSpec())
        .withRollup(false)
        .build();
  }

  public RowSignature getInputRowSignature()
  {
    Builder rsBuilder = RowSignature.builder();
    for (DimensionSchema dimensionSchema : getInputRowSchema().getDimensionsSpec().getDimensions()) {
      rsBuilder.add(dimensionSchema.getName(), dimensionSchema.getColumnType());
    }
    return rsBuilder.build();
  }

  public abstract Iterable<InputRow> getRows();

  public abstract InputRowSchema getInputRowSchema();

  public abstract List<AggregatorFactory> getMetrics();

}