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

package io.druid.indexer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.DoubleDimensionSchema;
import io.druid.data.input.impl.FloatDimensionSchema;
import io.druid.data.input.impl.LongDimensionSchema;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.hll.HyperLogLogCollector;
import io.druid.jackson.AggregatorsModule;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregator;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
public class InputRowSerdeTest
{
  private long timestamp;
  private List<String> dims;
  private Map<String, Object> event;

  {
    new AggregatorsModule(); //registers ComplexMetric serde for hyperUnique
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public InputRowSerdeTest()
  {
    this.timestamp = System.currentTimeMillis();
    this.dims = ImmutableList.of("dim_non_existing", "d1", "d2", "d3", "d4", "d5");
    this.event = Maps.newHashMap();
    event.put("d1", "d1v");
    event.put("d2", ImmutableList.of("d2v1", "d2v2"));
    event.put("d3", 200L);
    event.put("d4", 300.1f);
    event.put("d5", 400.5d);
    event.put("m1", 5.0f);
    event.put("m2", 100L);
    event.put("m3", "m3v");
  }

  @Test
  public void testSerde()
  {
    // Prepare the mocks & set close() call count expectation to 1
    final Aggregator mockedAggregator = EasyMock.createMock(DoubleSumAggregator.class);
    EasyMock.expect(mockedAggregator.getDouble()).andReturn(0d).times(1);
    mockedAggregator.aggregate();
    EasyMock.expectLastCall().times(1);
    mockedAggregator.close();
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(mockedAggregator);

    InputRow in = new MapBasedInputRow(
        timestamp,
        dims,
        event
    );

    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new DoubleSumAggregatorFactory("agg_non_existing", "agg_non_existing_in"),
        new DoubleSumAggregatorFactory("m1out", "m1"),
        new LongSumAggregatorFactory("m2out", "m2"),
        new HyperUniquesAggregatorFactory("m3out", "m3"),
        new LongSumAggregatorFactory("unparseable", "m3"), // Unparseable from String to Long
        new DoubleSumAggregatorFactory("mockedAggregator", "m4") {
          @Override
          public Aggregator factorize(ColumnSelectorFactory metricFactory)
          {
            return mockedAggregator;
          }
        }
    };

    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2"),
            new LongDimensionSchema("d3"),
            new FloatDimensionSchema("d4"),
            new DoubleDimensionSchema("d5")
        ),
        null,
        null
    );

    byte[] data = InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, aggregatorFactories, false); // Ignore Unparseable aggregator
    InputRow out = InputRowSerde.fromBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), data, aggregatorFactories);

    Assert.assertEquals(timestamp, out.getTimestampFromEpoch());
    Assert.assertEquals(dims, out.getDimensions());
    Assert.assertEquals(Collections.EMPTY_LIST, out.getDimension("dim_non_existing"));
    Assert.assertEquals(ImmutableList.of("d1v"), out.getDimension("d1"));
    Assert.assertEquals(ImmutableList.of("d2v1", "d2v2"), out.getDimension("d2"));
    Assert.assertEquals(200L, out.getRaw("d3"));
    Assert.assertEquals(300.1f, out.getRaw("d4"));
    Assert.assertEquals(400.5d, out.getRaw("d5"));

    Assert.assertEquals(0.0f, out.getMetric("agg_non_existing").floatValue(), 0.00001);
    Assert.assertEquals(5.0f, out.getMetric("m1out").floatValue(), 0.00001);
    Assert.assertEquals(100L, out.getMetric("m2out"));
    Assert.assertEquals(1, ((HyperLogLogCollector) out.getRaw("m3out")).estimateCardinality(), 0.001);
    Assert.assertEquals(0L, out.getMetric("unparseable"));

    EasyMock.verify(mockedAggregator);
  }

  @Test
  public void testThrowParseExceptions()
  {
    InputRow in = new MapBasedInputRow(
        timestamp,
        dims,
        event
    );
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new DoubleSumAggregatorFactory("agg_non_existing", "agg_non_existing_in"),
        new DoubleSumAggregatorFactory("m1out", "m1"),
        new LongSumAggregatorFactory("m2out", "m2"),
        new HyperUniquesAggregatorFactory("m3out", "m3"),
        new LongSumAggregatorFactory("unparseable", "m3") // Unparseable from String to Long
    };

    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2"),
            new LongDimensionSchema("d3"),
            new FloatDimensionSchema("d4"),
            new DoubleDimensionSchema("d5")
        ),
        null,
        null
    );

    expectedException.expect(ParseException.class);
    expectedException.expectMessage("Encountered parse error for aggregator[unparseable]");
    InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, aggregatorFactories, true);
  }

  @Test
  public void testDimensionParseExceptions()
  {
    InputRow in = new MapBasedInputRow(
        timestamp,
        dims,
        event
    );
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new LongSumAggregatorFactory("m2out", "m2")
    };

    expectedException.expect(ParseException.class);
    expectedException.expectMessage("could not convert value [d1v] to long");
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new LongDimensionSchema("d1")
        ),
        null,
        null
    );
    InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, aggregatorFactories, true);

    expectedException.expect(ParseException.class);
    expectedException.expectMessage("could not convert value [d1v] to float");
    dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new FloatDimensionSchema("d1")
        ),
        null,
        null
    );
    InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, aggregatorFactories, true);

    expectedException.expect(ParseException.class);
    expectedException.expectMessage("could not convert value [d1v] to double");
    dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new DoubleDimensionSchema("d1")
        ),
        null,
        null
    );
    InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, aggregatorFactories, true);
  }
}
