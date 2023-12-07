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

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableObjectColumnValueSelector;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.util.Comparator;

public class NestedDataColumnHandlerV4 implements DimensionHandler<StructuredData, StructuredData, StructuredData>
{
  private static Comparator<ColumnValueSelector> COMPARATOR = (s1, s2) ->
      StructuredData.COMPARATOR.compare(
          StructuredData.wrap(s1.getObject()),
          StructuredData.wrap(s2.getObject())
      );

  private final String name;

  public NestedDataColumnHandlerV4(String name)
  {
    this.name = name;
  }

  @Override
  public String getDimensionName()
  {
    return name;
  }

  @Override
  public DimensionSpec getDimensionSpec()
  {
    return new DefaultDimensionSpec(name, name, ColumnType.NESTED_DATA);
  }

  @Override
  public DimensionSchema getDimensionSchema(ColumnCapabilities capabilities)
  {
    return new NestedDataColumnSchema(name, 4);
  }

  @Override
  public DimensionIndexer<StructuredData, StructuredData, StructuredData> makeIndexer(boolean useMaxMemoryEstimates)
  {
    return new NestedDataColumnIndexerV4();
  }

  @Override
  public DimensionMergerV9 makeMerger(
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilities,
      ProgressIndicator progress,
      Closer closer
  )
  {
    return new NestedDataColumnMergerV4(name, indexSpec, segmentWriteOutMedium, closer);
  }

  @Override
  public int getLengthOfEncodedKeyComponent(StructuredData dimVals)
  {
    // this is called in one place, OnheapIncrementalIndex, where returning 0 here means the value is null
    // so the actual value we return here doesn't matter. we should consider refactoring this to a boolean
    return 1;
  }

  @Override
  public Comparator<ColumnValueSelector> getEncodedValueSelectorComparator()
  {
    return COMPARATOR;
  }

  @Override
  public SettableColumnValueSelector makeNewSettableEncodedValueSelector()
  {
    return new SettableObjectColumnValueSelector();
  }
}
