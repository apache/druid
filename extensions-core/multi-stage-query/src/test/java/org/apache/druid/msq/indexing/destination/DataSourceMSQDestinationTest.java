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

package org.apache.druid.msq.indexing.destination;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DataSourceMSQDestinationTest
{

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(DataSourceMSQDestination.class)
                  .withNonnullFields("dataSource", "segmentGranularity", "segmentSortOrder", "dimensionSchemas")
                  .withPrefabValues(
                      Map.class,
                      ImmutableMap.of(
                          "language",
                          new StringDimensionSchema(
                              "language",
                              DimensionSchema.MultiValueHandling.SORTED_ARRAY,
                              false
                          )
                      ),
                      ImmutableMap.of(
                          "region",
                          new StringDimensionSchema(
                              "region",
                              DimensionSchema.MultiValueHandling.SORTED_ARRAY,
                              false
                          )
                      )
                  )
                  .withPrefabValues(
                      List.class,
                      ImmutableList.of(
                          new AggregateProjectionSpec(
                              "projection1",
                              null,
                              VirtualColumns.EMPTY,
                              ImmutableList.of(
                                  new StringDimensionSchema("region")
                              ),
                              new AggregatorFactory[]{
                                  new CountAggregatorFactory("count")
                              }
                          )
                      ),
                      ImmutableList.of(
                          new AggregateProjectionSpec(
                              "projection2",
                              null,
                              VirtualColumns.create(
                                  Granularities.toVirtualColumn(
                                      Granularities.HOUR,
                                      Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
                                  )
                              ),
                              ImmutableList.of(
                                  new StringDimensionSchema("language"),
                                  new StringDimensionSchema("region")
                              ),
                              new AggregatorFactory[]{
                                  new CountAggregatorFactory("count"),
                                  new LongSumAggregatorFactory("sum_added", "added")
                              }
                          )
                      )
                  )
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testBackwardCompatibility() throws JsonProcessingException
  {
    DataSourceMSQDestination destination = new DataSourceMSQDestination("foo1", Granularities.ALL, null, null, null, null, null);
    Assert.assertEquals(SegmentGenerationStageSpec.instance(), destination.getTerminalStageSpec());

    DataSourceMSQDestination dataSourceMSQDestination = new DefaultObjectMapper().readValue(
        "{\"type\":\"dataSource\",\"dataSource\":\"datasource1\",\"segmentGranularity\":\"DAY\",\"rowsInTaskReport\":0,\"destinationResource\":{\"empty\":false,\"present\":true}}",
        DataSourceMSQDestination.class
    );
    Assert.assertEquals(SegmentGenerationStageSpec.instance(), dataSourceMSQDestination.getTerminalStageSpec());
  }
}
