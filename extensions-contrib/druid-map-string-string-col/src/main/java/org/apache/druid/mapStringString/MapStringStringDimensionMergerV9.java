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

package org.apache.druid.mapStringString;

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionMergerV9;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.IndexableAdapter;
import org.apache.druid.segment.ProgressIndicator;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.ComplexColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.List;

public class MapStringStringDimensionMergerV9 implements DimensionMergerV9
{
  private final GenericColumnSerializer serializer;

  public MapStringStringDimensionMergerV9(
      String dimensionName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      IndexSpec indexSpec,
      ProgressIndicator progressIndicator,
      Closer closer
  )
  {
    this.serializer = new MapStringStringColumnSerializer(
        segmentWriteOutMedium,
        dimensionName,
        indexSpec.getBitmapSerdeFactory(),
        indexSpec.getDimensionCompression(),
        MapStringStringComplexMetricSerde.JSON_MAPPER
    );
  }

  @Override
  public ColumnDescriptor makeColumnDescriptor()
  {
    return new ColumnDescriptor.Builder()
        .setValueType(ValueType.COMPLEX)
        .setHasMultipleValues(false)
        .addSerde(ComplexColumnPartSerde.serializerBuilder()
                                        .withTypeName(MapStringStringDruidModule.TYPE_NAME)
                                        .withDelegate(serializer)
                                        .build()
        )
        .build();
  }

  @Override
  public void writeMergedValueDictionary(List<IndexableAdapter> adapters)
  {

  }

  @Override
  public ColumnValueSelector convertSortedSegmentRowValuesToMergedRowValues(
      int segmentIndex,
      ColumnValueSelector source
  )
  {
    return source;
  }

  @Override
  public void processMergedRow(ColumnValueSelector selector) throws IOException
  {
    serializer.serialize(selector);
  }

  @Override
  public void writeIndexes(@Nullable List<IntBuffer> segmentRowNumConversions)
  {

  }

  @Override
  public boolean canSkip()
  {
    return false;
  }
}
