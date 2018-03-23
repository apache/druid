/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import io.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.List;

/**
 * Common base of {@link LongDimensionMergerV9}, {@link DoubleDimensionMergerV9} and {@link FloatDimensionMergerV9}.
 */
public abstract class NumericDimensionMergerV9 implements DimensionMergerV9
{
  protected final String dimensionName;
  protected final IndexSpec indexSpec;
  protected final SegmentWriteOutMedium segmentWriteOutMedium;

  protected final GenericColumnSerializer serializer;

  NumericDimensionMergerV9(
      String dimensionName,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium
  )
  {
    this.dimensionName = dimensionName;
    this.indexSpec = indexSpec;
    this.segmentWriteOutMedium = segmentWriteOutMedium;

    try {
      serializer = setupEncodedValueWriter();
      serializer.open();
    }
    catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  abstract GenericColumnSerializer setupEncodedValueWriter();

  @Override
  public final void writeMergedValueDictionary(List<IndexableAdapter> adapters)
  {
    // numeric values have no additional metadata
  }

  @Override
  public final ColumnValueSelector convertSortedSegmentRowValuesToMergedRowValues(
      int segmentIndex,
      ColumnValueSelector source
  )
  {
    return source;
  }

  @Override
  public final void processMergedRow(ColumnValueSelector selector) throws IOException
  {
    serializer.serialize(selector);
  }

  @Override
  public final void writeIndexes(@Nullable List<IntBuffer> segmentRowNumConversions)
  {
    // numeric values have no indices to write
  }

  @Override
  public final boolean canSkip()
  {
    return false;
  }
}
