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

package org.apache.druid.query.aggregation.datasketches.hll.vector;

import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildBufferAggregatorHelper;
import org.apache.druid.segment.VectorColumnProcessorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

public class HllSketchBuildVectorProcessorFactory implements VectorColumnProcessorFactory<HllSketchBuildVectorProcessor>
{
  private final HllSketchBuildBufferAggregatorHelper helper;
  private final StringEncoding stringEncoding;

  public HllSketchBuildVectorProcessorFactory(
      final HllSketchBuildBufferAggregatorHelper helper,
      final StringEncoding stringEncoding
  )
  {
    this.helper = helper;
    this.stringEncoding = stringEncoding;
  }

  @Override
  public HllSketchBuildVectorProcessor makeSingleValueDimensionProcessor(
      ColumnCapabilities capabilities,
      SingleValueDimensionVectorSelector selector
  )
  {
    return new SingleValueStringHllSketchBuildVectorProcessor(helper, stringEncoding, selector);
  }

  @Override
  public HllSketchBuildVectorProcessor makeMultiValueDimensionProcessor(
      ColumnCapabilities capabilities,
      MultiValueDimensionVectorSelector selector
  )
  {
    return new MultiValueStringHllSketchBuildVectorProcessor(helper, stringEncoding, selector);
  }

  @Override
  public HllSketchBuildVectorProcessor makeFloatProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
  {
    // No specialized "float" version, for consistency with HllSketchBuildAggregator#updateSketch (it treats floats
    // and doubles identically).
    return new DoubleHllSketchBuildVectorProcessor(helper, selector);
  }

  @Override
  public HllSketchBuildVectorProcessor makeDoubleProcessor(
      ColumnCapabilities capabilities,
      VectorValueSelector selector
  )
  {
    return new DoubleHllSketchBuildVectorProcessor(helper, selector);
  }

  @Override
  public HllSketchBuildVectorProcessor makeLongProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
  {
    return new LongHllSketchBuildVectorProcessor(helper, selector);
  }

  @Override
  public HllSketchBuildVectorProcessor makeObjectProcessor(
      ColumnCapabilities capabilities,
      VectorObjectSelector selector
  )
  {
    return new ObjectHllSketchBuildVectorProcessor(helper, stringEncoding, selector);
  }
}
