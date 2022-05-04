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

public class KllDoublesSketchAggregatorFactoryTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(KllDoublesSketchAggregatorFactory.class)
                  .withNonnullFields("name", "fieldName")
                  .withIgnoredFields("cacheTypeId")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(new NamedType(KllDoublesSketchAggregatorFactory.class, KllSketchModule.DOUBLES_SKETCH));
    final KllDoublesSketchAggregatorFactory factory = new KllDoublesSketchAggregatorFactory(
        "myFactory",
        "myField",
        1024,
        1000L
    );
    final byte[] json = mapper.writeValueAsBytes(factory);
    final KllDoublesSketchAggregatorFactory fromJson = (KllDoublesSketchAggregatorFactory) mapper.readValue(
        json,
        AggregatorFactory.class
    );
    Assert.assertEquals(factory, fromJson);
  }

  @Test
  public void testDefaultParams()
  {
    final KllDoublesSketchAggregatorFactory factory = new KllDoublesSketchAggregatorFactory(
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
    KllDoublesSketchAggregatorFactory factory = new KllDoublesSketchAggregatorFactory(
        "myFactory",
        "myField",
        200,
        null
    );
    Assert.assertEquals(1644, factory.guessAggregatorHeapFootprint(1));
    Assert.assertEquals(1644, factory.guessAggregatorHeapFootprint(100));
    Assert.assertEquals(3428, factory.guessAggregatorHeapFootprint(1000));
    Assert.assertEquals(6388, factory.guessAggregatorHeapFootprint(1_000_000_000_000L));
  }

  @Test
  public void testMaxIntermediateSize()
  {
    KllDoublesSketchAggregatorFactory factory = new KllDoublesSketchAggregatorFactory(
        "myFactory",
        "myField",
        200,
        null
    );
    Assert.assertEquals(5708, factory.getMaxIntermediateSize());

    factory = new KllDoublesSketchAggregatorFactory(
        "myFactory",
        "myField",
        200,
        1_000_000_000_000L
    );
    Assert.assertEquals(6388, factory.getMaxIntermediateSize());
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
                  new KllDoublesSketchAggregatorFactory("doublesSketch", "col", 8, 1000000000L),
                  new KllDoublesSketchMergeAggregatorFactory("doublesSketchMerge", 8)
              )
              .postAggregators(
                  new FieldAccessPostAggregator("doublesSketch-access", "doublesSketch"),
                  new FinalizingFieldAccessPostAggregator("doublesSketch-finalize", "doublesSketch"),
                  new FieldAccessPostAggregator("doublesSketchMerge-access", "doublesSketchMerge"),
                  new FinalizingFieldAccessPostAggregator("doublesSketchMerge-finalize", "doublesSketchMerge")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("doublesSketch", null)
                    .add("doublesSketchMerge", null)
                    .add("doublesSketch-access", KllSketchModule.DOUBLES_TYPE)
                    .add("doublesSketch-finalize", ColumnType.LONG)
                    .add("doublesSketchMerge-access", KllSketchModule.DOUBLES_TYPE)
                    .add("doublesSketchMerge-finalize", ColumnType.LONG)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
