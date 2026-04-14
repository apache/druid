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

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import java.util.Comparator;
import java.util.Map;

/**
 * Returns a base64 encoded string of a given {@link ArrayOfDoublesSketch}.
 * This is a string returned by  encoding the output of toByteArray() using Base64 method of the sketch.
 * This can be useful for debugging and using the sketch output in other operations.
 */
public class ArrayOfDoublesSketchToBase64StringPostAggregator extends ArrayOfDoublesSketchUnaryPostAggregator
{

  @JsonCreator
  public ArrayOfDoublesSketchToBase64StringPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field
  )
  {
    super(name, field);
  }

  @Override
  public String compute(final Map<String, Object> combinedAggregators)
  {
    final ArrayOfDoublesSketch sketch = (ArrayOfDoublesSketch) getField().compute(combinedAggregators);
    return StringUtils.encodeBase64String(sketch.toByteArray());
  }

  @Override
  public ColumnType getType(ColumnInspector signature)
  {
    return ColumnType.STRING;
  }

  @Override
  public Comparator<String> getComparator()
  {
    throw new IAE("Comparing sketch summaries is not supported");
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.ARRAY_OF_DOUBLES_SKETCH_TO_BASE64_STRING_CACHE_TYPE_ID)
        .appendCacheable(getField())
        .build();
  }
}
