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

import com.google.common.collect.Maps;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedLongs;

import java.util.Map;

public class ComparableDimensionSelector implements DimensionSelector
{
  private final ObjectColumnSelector selector;
  private final ExtractionFn extractionFn;
  private final ColumnCapabilities capabilities;
  private final Map<Comparable, String> remappedValues = Maps.newHashMap();

  public ComparableDimensionSelector(Object selector, ExtractionFn extractionFn, ColumnCapabilities capabilities)
  {
    this.selector = (ObjectColumnSelector) selector;
    this.extractionFn = extractionFn;
    this.capabilities = capabilities;
  }

  @Override
  public IndexedInts getRow()
  {
    throw new UnsupportedOperationException("float column does not support getRow");
  }

  @Override
  public int getValueCardinality()
  {
    throw new UnsupportedOperationException("comparable column does not support getValueCardinality");
  }

  @Override
  public int lookupId(String name)
  {
    throw new UnsupportedOperationException("comparable column does not support lookupId");
  }

  @Override
  public String lookupName(int id)
  {
    throw new UnsupportedOperationException("comparable column does not support lookupName");
  }

  @Override
  public IndexedFloats getFloatRow()
  {
    throw new UnsupportedOperationException("comparable column does not support getFloatRow");
  }

  @Override
  public Comparable getExtractedValueFloat(float val)
  {
    throw new UnsupportedOperationException("comparable column does not support getExtractedValueComparable");
  }

  @Override
  public IndexedLongs getLongRow()
  {
    throw new UnsupportedOperationException("comparable column does not support getFloatRow");
  }

  @Override
  public Comparable getExtractedValueLong(long val)
  {
    throw new UnsupportedOperationException("comparable column does not support getExtractedValueFloat");
  }

  @Override
  public Comparable getComparableRow()
  {
    return (Comparable) selector.get();
  }

  @Override
  public Comparable getExtractedValueComparable(Comparable val)
  {
    if (extractionFn == null) {
      return val;
    }

    String extractedVal = remappedValues.get(val);
    if (extractedVal == null) {
      extractedVal = extractionFn.apply(val);
      remappedValues.put(val, extractedVal);
    }
    return extractedVal;
  }

  @Override
  public ColumnCapabilities getDimCapabilities()
  {
    return capabilities;
  }

}
