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

package org.apache.druid.segment.loading;

import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Empty segment with nothing but a time column, used when {@link SegmentLoaderConfig#isVirtualStorage()} is true
 * to create synthetic segments for {@link org.apache.druid.query.metadata.metadata.SegmentMetadataQuery} so to not
 * force download of segments from deep storage when computing the SQL schema.
 */
public class VirtualPlaceholderSegment extends RowBasedSegment<Row>
{
  private final DataSegment dataSegment;

  public VirtualPlaceholderSegment(DataSegment dataSegment)
  {
    super(
        Sequences.empty(),
        RowAdapters.standardRow(),
        RowSignature.builder().addTimeColumn().build()
    );
    this.dataSegment = dataSegment;
  }

  @Override
  public SegmentId getId()
  {
    return dataSegment.getId();
  }

  @Override
  @Nonnull
  public Interval getDataInterval()
  {
    return dataSegment.getInterval();
  }

  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    if (PhysicalSegmentInspector.class.equals(clazz)) {
      return (T) EmptyPhysicalInspector.INSTANCE;
    }
    return super.as(clazz);
  }

  private static class EmptyPhysicalInspector implements PhysicalSegmentInspector
  {
    private static final EmptyPhysicalInspector INSTANCE = new EmptyPhysicalInspector();

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      if (ColumnHolder.TIME_COLUMN_NAME.equals(column)) {
        return ColumnCapabilitiesImpl.createDefault().setType(ColumnType.LONG);
      }
      return null;
    }

    @Nullable
    @Override
    public Metadata getMetadata()
    {
      return null;
    }

    @Nullable
    @Override
    public Comparable getMinValue(String column)
    {
      return null;
    }

    @Nullable
    @Override
    public Comparable getMaxValue(String column)
    {
      return null;
    }

    @Override
    public int getDimensionCardinality(String column)
    {
      return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
    }

    @Override
    public int getNumRows()
    {
      return 0;
    }
  }
}
