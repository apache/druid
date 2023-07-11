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

package org.apache.druid.query.aggregation.datasketches.kll;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class KllFloatsSketchAggregatorFactoryTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(KllFloatsSketchAggregatorFactory.class)
                  .withNonnullFields("name", "fieldName")
                  .withIgnoredFields("cacheTypeId")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(new NamedType(KllFloatsSketchAggregatorFactory.class, KllSketchModule.FLOATS_SKETCH));
    final KllFloatsSketchAggregatorFactory factory = new KllFloatsSketchAggregatorFactory(
        "myFactory",
        "myField",
        1024,
        1000L
    );
    final byte[] json = mapper.writeValueAsBytes(factory);
    final KllFloatsSketchAggregatorFactory fromJson = (KllFloatsSketchAggregatorFactory) mapper.readValue(
        json,
        AggregatorFactory.class
    );
    Assert.assertEquals(factory, fromJson);
  }

  @Test
  public void testDefaultParams()
  {
    final KllFloatsSketchAggregatorFactory factory = new KllFloatsSketchAggregatorFactory(
        "myFactory",
        "myField",
        null,
        null
    );

    Assert.assertEquals(KllSketchAggregatorFactory.DEFAULT_K, factory.getK());
    Assert.assertEquals(KllSketchAggregatorFactory.DEFAULT_MAX_STREAM_LENGTH, factory.getMaxStreamLength());
  }

  @Test
  public void testGuessAggregatorHeapFootprint()
  {
    KllFloatsSketchAggregatorFactory factory = new KllFloatsSketchAggregatorFactory(
        "myFactory",
        "myField",
        200,
        null
    );
    Assert.assertEquals(836, factory.guessAggregatorHeapFootprint(1));
    Assert.assertEquals(836, factory.guessAggregatorHeapFootprint(100));
    Assert.assertEquals(1732, factory.guessAggregatorHeapFootprint(1000));
    Assert.assertEquals(3272, factory.guessAggregatorHeapFootprint(1_000_000_000_000L));
  }

  @Test
  public void testMaxIntermediateSize()
  {
    KllFloatsSketchAggregatorFactory factory = new KllFloatsSketchAggregatorFactory(
        "myFactory",
        "myField",
        200,
        null
    );
    Assert.assertEquals(2912, factory.getMaxIntermediateSize());

    factory = new KllFloatsSketchAggregatorFactory(
        "myFactory",
        "myField",
        200,
        1_000_000_000_000L
    );
    Assert.assertEquals(3272, factory.getMaxIntermediateSize());
  }

  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new CountAggregatorFactory("count"),
                  new KllFloatsSketchAggregatorFactory("floatsSketch", "col", 8, 1000000000L),
                  new KllFloatsSketchMergeAggregatorFactory("floatsSketchMerge", 8)
              )
              .postAggregators(
                  new FieldAccessPostAggregator("floatsSketch-access", "floatsSketch"),
                  new FinalizingFieldAccessPostAggregator("floatsSketch-finalize", "floatsSketch"),
                  new FieldAccessPostAggregator("floatsSketchMerge-access", "floatsSketchMerge"),
                  new FinalizingFieldAccessPostAggregator("floatsSketchMerge-finalize", "floatsSketchMerge")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("floatsSketch", null)
                    .add("floatsSketchMerge", null)
                    .add("floatsSketch-access", KllSketchModule.FLOATS_TYPE)
                    .add("floatsSketch-finalize", ColumnType.LONG)
                    .add("floatsSketchMerge-access", KllSketchModule.FLOATS_TYPE)
                    .add("floatsSketchMerge-finalize", ColumnType.LONG)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
