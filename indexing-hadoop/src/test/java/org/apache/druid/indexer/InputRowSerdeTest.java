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

package org.apache.druid.indexer;

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.jackson.AggregatorsModule;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregator;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class InputRowSerdeTest
{
  private long timestamp;
  private List<String> dims;
  private Map<String, Object> event;

  static {
    NullHandling.initializeForTests();
    new AggregatorsModule(); //registers ComplexMetric serde for hyperUnique
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public InputRowSerdeTest()
  {
    this.timestamp = System.currentTimeMillis();
    this.dims = ImmutableList.of("dim_non_existing", "d1", "d2", "d3", "d4", "d5");
    this.event = new HashMap<>();
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
    EasyMock.expect(mockedAggregator.isNull()).andReturn(false).times(1);
    EasyMock.expect(mockedAggregator.getDouble()).andReturn(0d).times(1);
    mockedAggregator.aggregate();
    EasyMock.expectLastCall().times(1);
    mockedAggregator.close();
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(mockedAggregator);

    final Aggregator mockedNullAggregator = EasyMock.createMock(DoubleSumAggregator.class);
    EasyMock.expect(mockedNullAggregator.isNull()).andReturn(true).times(1);
    mockedNullAggregator.aggregate();
    EasyMock.expectLastCall().times(1);
    mockedNullAggregator.close();
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(mockedNullAggregator);

    final AggregatorFactory mockedAggregatorFactory = EasyMock.createMock(AggregatorFactory.class);
    EasyMock.expect(mockedAggregatorFactory.factorize(EasyMock.anyObject(ColumnSelectorFactory.class))).andReturn(mockedAggregator);
    EasyMock.expect(mockedAggregatorFactory.getTypeName()).andReturn("double").anyTimes();
    EasyMock.expect(mockedAggregatorFactory.getName()).andReturn("mockedAggregator").anyTimes();

    final AggregatorFactory mockedNullAggregatorFactory = EasyMock.createMock(AggregatorFactory.class);
    EasyMock.expect(mockedNullAggregatorFactory.factorize(EasyMock.anyObject(ColumnSelectorFactory.class))).andReturn(mockedNullAggregator);
    EasyMock.expect(mockedNullAggregatorFactory.getName()).andReturn("mockedNullAggregator").anyTimes();
    EasyMock.expect(mockedNullAggregatorFactory.getTypeName()).andReturn("double").anyTimes();

    EasyMock.replay(mockedAggregatorFactory, mockedNullAggregatorFactory);

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
        mockedAggregatorFactory,
        mockedNullAggregatorFactory
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

    byte[] data = InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, aggregatorFactories)
                               .getSerializedRow(); // Ignore Unparseable aggregator
    InputRow out = InputRowSerde.fromBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), data, aggregatorFactories);

    Assert.assertEquals(timestamp, out.getTimestampFromEpoch());
    Assert.assertEquals(dims, out.getDimensions());
    Assert.assertEquals(Collections.emptyList(), out.getDimension("dim_non_existing"));
    Assert.assertEquals(ImmutableList.of("d1v"), out.getDimension("d1"));
    Assert.assertEquals(ImmutableList.of("d2v1", "d2v2"), out.getDimension("d2"));
    Assert.assertEquals(200L, out.getRaw("d3"));
    Assert.assertEquals(300.1f, out.getRaw("d4"));
    Assert.assertEquals(400.5d, out.getRaw("d5"));

    Assert.assertEquals(NullHandling.defaultDoubleValue(), out.getMetric("agg_non_existing"));
    Assert.assertEquals(5.0f, out.getMetric("m1out").floatValue(), 0.00001);
    Assert.assertEquals(100L, out.getMetric("m2out"));
    Assert.assertEquals(1, ((HyperLogLogCollector) out.getRaw("m3out")).estimateCardinality(), 0.001);
    Assert.assertEquals(NullHandling.defaultLongValue(), out.getMetric("unparseable"));

    EasyMock.verify(mockedAggregator);
    EasyMock.verify(mockedNullAggregator);
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

    InputRowSerde.SerializeResult result = InputRowSerde.toBytes(
        InputRowSerde.getTypeHelperMap(dimensionsSpec),
        in,
        aggregatorFactories
    );
    Assert.assertEquals(
        Collections.singletonList("Unable to parse value[m3v] for field[m3]"),
        result.getParseExceptionMessages()
    );
  }

  @Test
  public void testDimensionParseExceptions()
  {
    InputRowSerde.SerializeResult result;
    InputRow in = new MapBasedInputRow(
        timestamp,
        dims,
        event
    );
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new LongSumAggregatorFactory("m2out", "m2")
    };

    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Collections.singletonList(
            new LongDimensionSchema("d1")
        ),
        null,
        null
    );
    result = InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, aggregatorFactories);
    Assert.assertEquals(
        Collections.singletonList("could not convert value [d1v] to long"),
        result.getParseExceptionMessages()
    );

    dimensionsSpec = new DimensionsSpec(
        Collections.singletonList(
            new FloatDimensionSchema("d1")
        ),
        null,
        null
    );
    result = InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, aggregatorFactories);
    Assert.assertEquals(
        Collections.singletonList("could not convert value [d1v] to float"),
        result.getParseExceptionMessages()
    );

    dimensionsSpec = new DimensionsSpec(
        Collections.singletonList(
            new DoubleDimensionSchema("d1")
        ),
        null,
        null
    );
    result = InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, aggregatorFactories);
    Assert.assertEquals(
        Collections.singletonList("could not convert value [d1v] to double"),
        result.getParseExceptionMessages()
    );
  }

  @Test
  public void testDimensionNullOrDefaultForNumerics()
  {
    HashMap<String, Object> eventWithNulls = new HashMap<>();
    eventWithNulls.put("d1", null);
    eventWithNulls.put("d2", Arrays.asList("d2v1", "d2v2"));
    eventWithNulls.put("d3", null);
    eventWithNulls.put("d4", null);
    eventWithNulls.put("d5", null);

    InputRow in = new MapBasedInputRow(
        timestamp,
        dims,
        eventWithNulls
    );

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

    byte[] result = InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, new AggregatorFactory[0]).getSerializedRow();

    if (NullHandling.replaceWithDefault()) {
      long expected = 0;
      expected += 9;  // timestamp bytes + dims length
      expected += 18; // dim_non_existing writes: 1 16 1 bytes
      expected += 4;  // d1: writes 1 2 1 bytes
      expected += 14; // d2: writes 1 2 1 1 4 1 4 bytes
      expected += 11; // d3: writes 1 2 8 bytes
      expected += 7;  // d4: writes 1 2 4 bytes
      expected += 11; // d5: writes 1 2 8 bytes
      expected += 1;  // writes aggregator length

      Assert.assertEquals(expected, result.length);
      Assert.assertArrayEquals(new byte[] {0, 0, 0, 0, 0, 0, 0, 0}, Arrays.copyOfRange(result, 48, 56));
      Assert.assertArrayEquals(new byte[] {0, 0, 0, 0}, Arrays.copyOfRange(result, 59, 63));
      Assert.assertArrayEquals(new byte[] {0, 0, 0, 0, 0, 0, 0, 0}, Arrays.copyOfRange(result, 66, 74));
    } else {
      long expected = 9 + 18 + 4 + 14 + 4 + 4 + 4 + 1;

      Assert.assertEquals(expected, result.length);
      Assert.assertEquals(result[48], NullHandling.IS_NULL_BYTE);
      Assert.assertEquals(result[52], NullHandling.IS_NULL_BYTE);
      Assert.assertEquals(result[56], NullHandling.IS_NULL_BYTE);
    }
  }
}
