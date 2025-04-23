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

package org.apache.druid.sql.calcite.util.datasets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.primes.Primes;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Numbers extends MapBasedTestDataset
{
  protected Numbers()
  {
    this("numbers");
  }

  public Numbers(String name)
  {
    super(name);
  }

  @Override
  public final InputRowSchema getInputRowSchema()
  {
    return new InputRowSchema(
        new TimestampSpec(TIMESTAMP_COLUMN, "iso", null),
        new DimensionsSpec(
            ImmutableList.<DimensionSchema>builder()
                .add(new LongDimensionSchema("number"))
                .add(new LongDimensionSchema("is_prime"))
                .add(new DoubleDimensionSchema("inverse"))
                .add(new StringDimensionSchema("label"))
                .add(new StringDimensionSchema("letters", MultiValueHandling.ARRAY, null))
                .build()
        ),
        null
    );
  }

  @Override
  public List<AggregatorFactory> getMetrics()
  {
    return ImmutableList.of(
        new CountAggregatorFactory("cnt")
    );
  }

  @Override
  public List<Map<String, Object>> getRawRows()
  {
    return ImmutableList.of(
        makeRow(1, "one"),
        makeRow(2, "two"),
        makeRow(3, "three"),
        makeRow(4, "four"),
        makeRow(5, "five"),
        makeRow(6, "six"),
        makeRow(7, "seven"),
        makeRow(8, "eight"),
        makeRow(9, "nine"),
        makeRow(10, "ten")
    );
  }

  private Map<String, Object> makeRow(int num, String string)
  {
    return ImmutableMap.<String, Object>builder()
        .put("t", "2000-01-01")
        .put("number", Integer.toString(num))
        .put("is_prime", Primes.isPrime(num))
        .put("inverse", 1.0D / num)
        .put("label", string)
        .put("letters", Arrays.asList(string.toCharArray()))
        .build();
  }
}
