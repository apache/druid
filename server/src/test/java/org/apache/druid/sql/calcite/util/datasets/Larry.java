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
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.AutoTypeColumnSchema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Larry extends MapBasedTestDataset
{
  protected Larry()
  {
    this("larry");
  }

  public Larry(String name)
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
                            "label",
                            "mv"
                        )
                    )
                )
                .add(new LongDimensionSchema("l1"))
                .add(new AutoTypeColumnSchema("l_arr", null))
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
        makeRow("[]", ImmutableList.of()),
        makeRow("[null]", Collections.singletonList(null)),
        makeRow("[1]", ImmutableList.of(1)),
        makeRow("[2,3]", ImmutableList.of(2, 3)),
        makeRow("null", null)
    );
  }

  private Map<String, Object> makeRow(String label, Object object)
  {
    Map<String, Object> ret = new HashMap<String, Object>();

    ret.put("t", "2000-01-01");
    ret.put("label", label);
    ret.put("l1", 11);
    ret.put("mv", object);
    ret.put("l_arr", object);
    return ret;

  }
}
