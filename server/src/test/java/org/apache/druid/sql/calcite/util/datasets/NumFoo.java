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
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;

import java.util.List;
import java.util.Map;

public class NumFoo extends MapBasedTestDataset
{
  protected NumFoo()
  {
    this("numfoo");
  }

  public NumFoo(String name)
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
                .addAll(
                    DimensionsSpec.getDefaultSchemas(
                        ImmutableList.of(
                            "dim1",
                            "dim2",
                            "dim3",
                            "dim4",
                            "dim5",
                            "dim6"
                        )
                    )
                )
                .add(new DoubleDimensionSchema("dbl1"))
                .add(new DoubleDimensionSchema("dbl2"))
                .add(new FloatDimensionSchema("f1"))
                .add(new FloatDimensionSchema("f2"))
                .add(new LongDimensionSchema("l1"))
                .add(new LongDimensionSchema("l2"))
                .build()
        ),
        null
    );
  }

  @Override
  public List<AggregatorFactory> getMetrics()
  {
    return ImmutableList.of(
        new CountAggregatorFactory("cnt"),
        new FloatSumAggregatorFactory("m1", "m1"),
        new DoubleSumAggregatorFactory("m2", "m2"),
        new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
    );
  }

  @Override
  public List<Map<String, Object>> getRawRows()
  {
    return ImmutableList.of(
        ImmutableMap.<String, Object>builder()
            .put("t", "2000-01-01")
            .put("m1", "1.0")
            .put("m2", "1.0")
            .put("dbl1", 1.0)
            .put("f1", 1.0f)
            .put("l1", 7L)
            .put("dim1", "")
            .put("dim2", ImmutableList.of("a"))
            .put("dim3", ImmutableList.of("a", "b"))
            .put("dim4", "a")
            .put("dim5", "aa")
            .put("dim6", "1")
            .build(),
        ImmutableMap.<String, Object>builder()
            .put("t", "2000-01-02")
            .put("m1", "2.0")
            .put("m2", "2.0")
            .put("dbl1", 1.7)
            .put("dbl2", 1.7)
            .put("f1", 0.1f)
            .put("f2", 0.1f)
            .put("l1", 325323L)
            .put("l2", 325323L)
            .put("dim1", "10.1")
            .put("dim2", ImmutableList.of())
            .put("dim3", ImmutableList.of("b", "c"))
            .put("dim4", "a")
            .put("dim5", "ab")
            .put("dim6", "2")
            .build(),
        ImmutableMap.<String, Object>builder()
            .put("t", "2000-01-03")
            .put("m1", "3.0")
            .put("m2", "3.0")
            .put("dbl1", 0.0)
            .put("dbl2", 0.0)
            .put("f1", 0.0)
            .put("f2", 0.0)
            .put("l1", 0)
            .put("l2", 0)
            .put("dim1", "2")
            .put("dim2", ImmutableList.of(""))
            .put("dim3", ImmutableList.of("d"))
            .put("dim4", "a")
            .put("dim5", "ba")
            .put("dim6", "3")
            .build(),
        ImmutableMap.<String, Object>builder()
            .put("t", "2001-01-01")
            .put("m1", "4.0")
            .put("m2", "4.0")
            .put("dim1", "1")
            .put("dim2", ImmutableList.of("a"))
            .put("dim3", ImmutableList.of(""))
            .put("dim4", "b")
            .put("dim5", "ad")
            .put("dim6", "4")
            .build(),
        ImmutableMap.<String, Object>builder()
            .put("t", "2001-01-02")
            .put("m1", "5.0")
            .put("m2", "5.0")
            .put("dim1", "def")
            .put("dim2", ImmutableList.of("abc"))
            .put("dim3", ImmutableList.of())
            .put("dim4", "b")
            .put("dim5", "aa")
            .put("dim6", "5")
            .build(),
        ImmutableMap.<String, Object>builder()
            .put("t", "2001-01-03")
            .put("m1", "6.0")
            .put("m2", "6.0")
            .put("dim1", "abc")
            .put("dim4", "b")
            .put("dim5", "ab")
            .put("dim6", "6")
            .build()
    );
  }
}
