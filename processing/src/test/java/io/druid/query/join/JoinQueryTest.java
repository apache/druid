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

package io.druid.query.join;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.VirtualColumns;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class JoinQueryTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws IOException
  {
    final JoinSpec leftChildSpec = new JoinSpec(
        JoinType.INNER,
        new AndPredicate(
            ImmutableList.of(
                new EqualPredicate(
                    new DimExtractPredicate(new DefaultDimensionSpec("src1", "dim1", "dim1")),
                    new DimExtractPredicate(new DefaultDimensionSpec("src2", "dim1", "dim1"))
                ),
                new EqualPredicate(
                    new AddPredicate(
                        new DimExtractPredicate(new DefaultDimensionSpec("src2", "dim2", "dim2")),
                        new LiteralPredicate("10")
                    ),
                    new AddPredicate(
                        new DimExtractPredicate(new DefaultDimensionSpec("src1", "dim2", "dim2")),
                        new DimExtractPredicate(new DefaultDimensionSpec("src1", "dim3", "dim3"))
                    )
                )
            )
        ),
        new DataInput(new TableDataSource("src1"), QueryRunnerTestHelper.firstToThird),
        new DataInput(new TableDataSource("src2"), QueryRunnerTestHelper.firstToThird)
    );

    final JoinSpec joinSpec = new JoinSpec(
        JoinType.INNER,
        new EqualPredicate(
            new DimExtractPredicate(new DefaultDimensionSpec("j1", "dim4", "dim4")),
            new DimExtractPredicate(new DefaultDimensionSpec("src3", "dim4", "dim4"))
        ),
        leftChildSpec,
        new DataInput(new TableDataSource("src3"), QueryRunnerTestHelper.firstToThird)
    );

    final Query query = JoinQuery.newBuilder()
                                 .setJoinSpec(joinSpec)
                                 .setGranularity(QueryRunnerTestHelper.dayGran)
                                 .setDimensions(
                                     ImmutableList.of(
                                         new DefaultDimensionSpec("src1", "dim5", "dim5"),
                                         new DefaultDimensionSpec("src2", "dim5", "dim5"),
                                         new DefaultDimensionSpec("src3", "dim5", "dim5")
                                     )
                                 )
                                 .setMetrics(
                                     ImmutableList.of(
                                         "met1", "met2", "met3"
                                     )
                                 )
                                 .setVirtualColumns(VirtualColumns.EMPTY)
                                 .build();

    final String json = JSON_MAPPER.writeValueAsString(query);
    final Query fromJson = JSON_MAPPER.readValue(json, Query.class);

    assertEquals(query, fromJson);
  }
}
