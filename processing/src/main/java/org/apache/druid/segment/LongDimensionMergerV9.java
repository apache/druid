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

package org.apache.druid.segment;

import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.ColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

public class LongDimensionMergerV9 extends NumericDimensionMergerV9
{

  LongDimensionMergerV9(String dimensionName, IndexSpec indexSpec, SegmentWriteOutMedium segmentWriteOutMedium)
  {
    super(dimensionName, indexSpec, segmentWriteOutMedium);
  }

  @Override
  GenericColumnSerializer setupEncodedValueWriter()
  {
    return IndexMergerV9.createLongColumnSerializer(segmentWriteOutMedium, dimensionName, indexSpec);
  }

  @Override
  public ColumnDescriptor makeColumnDescriptor()
  {
    final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
    builder.setValueType(ValueType.LONG);
    ColumnPartSerde serde = IndexMergerV9.createLongColumnPartSerde(serializer, indexSpec);
    builder.addSerde(serde);
    return builder.build();
  }
}
