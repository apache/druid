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

import com.google.common.collect.ImmutableMap;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.java.util.common.Pair;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.Indexed;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.Interval;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StringDimensionHandlerTest
{
  private static final Interval TEST_INTERVAL = Interval.parse("2015-01-01/2015-12-31");

  private static final IndexSpec INDEX_SPEC = new IndexSpec(
      new ConciseBitmapSerdeFactory(),
      CompressedObjectStrategy.CompressionStrategy.LZ4,
      CompressedObjectStrategy.CompressionStrategy.LZ4,
      CompressionFactory.LongEncodingStrategy.LONGS
  );


  private final List<String> dimensions = Arrays.asList("penguins", "predators");

  private static Pair<IncrementalIndexAdapter, IncrementalIndexAdapter> getAdapters(
      List<String> dims,
      Map<String, Object> event1,
      Map<String, Object> event2
  ) throws Exception
  {
    IncrementalIndex incrementalIndex1 = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMinTimestamp(TEST_INTERVAL.getStartMillis())
                .withDimensionsSpec(new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dims), null, null))
                .withMetrics(new CountAggregatorFactory("count"))
                .build()
        )
        .setMaxRowCount(1000)
        .buildOnheap();

    IncrementalIndex incrementalIndex2 = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMinTimestamp(TEST_INTERVAL.getStartMillis())
                .withDimensionsSpec(new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dims), null, null))
                .withMetrics(new CountAggregatorFactory("count"))
                .build()
        )
        .setMaxRowCount(1000)
        .buildOnheap();

    incrementalIndex1.add(new MapBasedInputRow(TEST_INTERVAL.getStartMillis(), dims, event1));
    incrementalIndex2.add(new MapBasedInputRow(TEST_INTERVAL.getStartMillis() + 3, dims, event2));

    IncrementalIndexAdapter adapter1 = new IncrementalIndexAdapter(
        TEST_INTERVAL,
        incrementalIndex1,
        INDEX_SPEC.getBitmapSerdeFactory().getBitmapFactory()
    );
    IncrementalIndexAdapter adapter2 = new IncrementalIndexAdapter(
        TEST_INTERVAL,
        incrementalIndex2,
        INDEX_SPEC.getBitmapSerdeFactory().getBitmapFactory()
    );

    return new Pair<>(adapter1, adapter2);
  }

  private static void validate(IncrementalIndexAdapter adapter1, IncrementalIndexAdapter adapter2) throws Exception
  {
    Map<String, DimensionHandler> handlers = adapter1.getDimensionHandlers();
    Indexed<String> dimNames1 = adapter1.getDimensionNames();
    Indexed<String> dimNames2 = adapter2.getDimensionNames();
    Iterator<Rowboat> iterator1 = adapter1.getRows().iterator();
    Iterator<Rowboat> iterator2 = adapter2.getRows().iterator();

    while (iterator1.hasNext()) {
      Rowboat r1 = iterator1.next();
      Rowboat r2 = iterator2.next();
      Object[] dims1 = r1.getDims();
      Object[] dims2 = r2.getDims();
      for (int i = 0; i < dims1.length; i++) {
        Object val1 = dims1[i];
        Object val2 = dims2[i];
        String name1 = dimNames1.get(i);
        String name2 = dimNames2.get(i);
        DimensionHandler handler = handlers.get(name1);
        handler.validateSortedEncodedKeyComponents(
            val1,
            val2,
            adapter1.getDimValueLookup(name1),
            adapter2.getDimValueLookup(name2)
        );
      }
    }
  }

  @Test
  public void testValidateSortedEncodedArrays() throws Exception
  {
    Map<String, Object> event1 = ImmutableMap.<String, Object>of(
        "penguins", Arrays.asList("adelie", "emperor"),
        "predators", Arrays.asList("seal")
    );
    Map<String, Object> event2 = ImmutableMap.<String, Object>of(
        "penguins", Arrays.asList("adelie", "emperor"),
        "predators", Arrays.asList("seal")
    );

    Pair<IncrementalIndexAdapter, IncrementalIndexAdapter> adapters = getAdapters(dimensions, event1, event2);
    IncrementalIndexAdapter adapter1 = adapters.lhs;
    IncrementalIndexAdapter adapter2 = adapters.rhs;

    validate(adapter1, adapter2);
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testValidateSortedDifferentEncodedArrays() throws Exception
  {
    Map<String, Object> event1 = ImmutableMap.<String, Object>of(
        "penguins", Arrays.asList("adelie", "emperor"),
        "predators", Collections.singletonList("seal")
    );
    Map<String, Object> event2 = ImmutableMap.<String, Object>of(
        "penguins", Arrays.asList("chinstrap", "gentoo"),
        "predators", Collections.singletonList("seal")
    );

    Pair<IncrementalIndexAdapter, IncrementalIndexAdapter> adapters = getAdapters(dimensions, event1, event2);
    IncrementalIndexAdapter adapter1 = adapters.lhs;
    IncrementalIndexAdapter adapter2 = adapters.rhs;

    exception.expect(SegmentValidationException.class);
    validate(adapter1, adapter2);
  }
}
