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

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.CloserRule;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class IncrementalIndexMultiValueSpecTest extends InitializedNullHandlingTest
{
  public final IncrementalIndexCreator indexCreator;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  public IncrementalIndexMultiValueSpecTest(String indexType) throws JsonProcessingException
  {
    indexCreator = closer.closeLater(new IncrementalIndexCreator(indexType, (builder, args) -> builder
        .setIndexSchema((IncrementalIndexSchema) args[0])
        .setMaxRowCount(10_000)
        .build()
    ));
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Collection<?> constructorFeeder()
  {
    return IncrementalIndexCreator.getAppendableIndexTypes();
  }

  @Test
  public void test() throws IndexSizeExceededException
  {
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("string1", DimensionSchema.MultiValueHandling.ARRAY, true),
            new StringDimensionSchema("string2", DimensionSchema.MultiValueHandling.SORTED_ARRAY, true),
            new StringDimensionSchema("string3", DimensionSchema.MultiValueHandling.SORTED_SET, true)
        ),
        null, null
    );
    IncrementalIndexSchema schema = new IncrementalIndexSchema(
        0,
        new TimestampSpec("ds", "auto", null),
        Granularities.ALL,
        VirtualColumns.EMPTY,
        dimensionsSpec,
        new AggregatorFactory[0],
        false
    );
    Map<String, Object> map = new HashMap<String, Object>()
    {
      @Override
      public Object get(Object key)
      {
        if (((String) key).startsWith("string")) {
          return Arrays.asList("xsd", "aba", "fds", "aba");
        }
        if (((String) key).startsWith("float")) {
          return Arrays.asList(3.92f, -2.76f, 42.153f, Float.NaN, -2.76f, -2.76f);
        }
        if (((String) key).startsWith("long")) {
          return Arrays.asList(-231238789L, 328L, 923L, 328L, -2L, 0L);
        }
        return null;
      }
    };
    IncrementalIndex<?> index = indexCreator.createIndex(schema);
    index.add(
        new MapBasedInputRow(
            0,
            Arrays.asList("string1", "string2", "string3", "float1", "float2", "float3", "long1", "long2", "long3"),
            map
        )
    );

    Row row = index.iterator().next();
    Assert.assertEquals(Lists.newArrayList("xsd", "aba", "fds", "aba"), row.getRaw("string1"));
    Assert.assertEquals(Lists.newArrayList("aba", "aba", "fds", "xsd"), row.getRaw("string2"));
    Assert.assertEquals(Lists.newArrayList("aba", "fds", "xsd"), row.getRaw("string3"));
  }
}
