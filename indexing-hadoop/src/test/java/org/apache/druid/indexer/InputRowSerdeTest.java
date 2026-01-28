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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeStrategies;
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
    EasyMock.expect(mockedAggregatorFactory.getIntermediateType()).andReturn(ColumnType.DOUBLE).anyTimes();
    EasyMock.expect(mockedAggregatorFactory.getName()).andReturn("mockedAggregator").anyTimes();

    final AggregatorFactory mockedNullAggregatorFactory = EasyMock.createMock(AggregatorFactory.class);
    EasyMock.expect(mockedNullAggregatorFactory.factorize(EasyMock.anyObject(ColumnSelectorFactory.class))).andReturn(mockedNullAggregator);
    EasyMock.expect(mockedNullAggregatorFactory.getName()).andReturn("mockedNullAggregator").anyTimes();
    EasyMock.expect(mockedNullAggregatorFactory.getIntermediateType()).andReturn(ColumnType.DOUBLE).anyTimes();

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
        )
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

    Assert.assertNull(out.getMetric("agg_non_existing"));
    Assert.assertEquals(5.0f, out.getMetric("m1out").floatValue(), 0.00001);
    Assert.assertEquals(100L, out.getMetric("m2out"));
    Assert.assertEquals(1, ((HyperLogLogCollector) out.getRaw("m3out")).estimateCardinality(), 0.001);
    Assert.assertNull(out.getMetric("unparseable"));

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
        )
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
        )
    );
    result = InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, aggregatorFactories);
    Assert.assertEquals(
        Collections.singletonList("Could not convert value [d1v] to long."),
        result.getParseExceptionMessages()
    );

    dimensionsSpec = new DimensionsSpec(
        Collections.singletonList(
            new FloatDimensionSchema("d1")
        )
    );
    result = InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, aggregatorFactories);
    Assert.assertEquals(
        Collections.singletonList("Could not convert value [d1v] to float."),
        result.getParseExceptionMessages()
    );

    dimensionsSpec = new DimensionsSpec(
        Collections.singletonList(
            new DoubleDimensionSchema("d1")
        )
    );
    result = InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, aggregatorFactories);
    Assert.assertEquals(
        Collections.singletonList("Could not convert value [d1v] to double."),
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
        )
    );

    byte[] result = InputRowSerde.toBytes(InputRowSerde.getTypeHelperMap(dimensionsSpec), in, new AggregatorFactory[0]).getSerializedRow();

    long expected = 9 + 18 + 4 + 14 + 4 + 4 + 4 + 1;

    Assert.assertEquals(expected, result.length);
    Assert.assertEquals(result[48], TypeStrategies.IS_NULL_BYTE);
    Assert.assertEquals(result[52], TypeStrategies.IS_NULL_BYTE);
    Assert.assertEquals(result[56], TypeStrategies.IS_NULL_BYTE);
  }

  @Test
  public void testMultiValueDimensionWithNulls()
  {
    HashMap<String, Object> eventWithNullInMultiValue = new HashMap<>();
    eventWithNullInMultiValue.put("d1", Arrays.asList("a", "b", null));

    InputRow in = new MapBasedInputRow(
        timestamp,
        ImmutableList.of("d1"),
        eventWithNullInMultiValue
    );

    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Collections.singletonList(new StringDimensionSchema("d1"))
    );

    byte[] data = InputRowSerde.toBytes(
        InputRowSerde.getTypeHelperMap(dimensionsSpec),
        in,
        new AggregatorFactory[0]
    ).getSerializedRow();

    InputRow out = InputRowSerde.fromBytes(
        InputRowSerde.getTypeHelperMap(dimensionsSpec),
        data,
        new AggregatorFactory[0]
    );

    // null should be preserved
    List<String> dimValues = out.getDimension("d1");
    Assert.assertEquals(3, dimValues.size());
    Assert.assertTrue(dimValues.contains("a"));
    Assert.assertTrue(dimValues.contains("b"));
    // getDimension() converts null to "null" string, so check raw value
    Object rawValue = out.getRaw("d1");
    Assert.assertTrue(rawValue instanceof List);
    @SuppressWarnings("unchecked")
    List<String> rawList = (List<String>) rawValue;
    Assert.assertTrue(rawList.contains(null));
    Assert.assertFalse(rawList.contains("null")); // Should NOT have "null" string
  }

  @Test
  public void testMultiValueDimensionWithMultipleNulls()
  {

    HashMap<String, Object> eventWithNullInMultiValue = new HashMap<>();
    eventWithNullInMultiValue.put("d1", Arrays.asList(null, "a", null, "b", null));

    InputRow in = new MapBasedInputRow(
        timestamp,
        ImmutableList.of("d1"),
        eventWithNullInMultiValue
    );

    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Collections.singletonList(new StringDimensionSchema("d1"))
    );

    byte[] data = InputRowSerde.toBytes(
        InputRowSerde.getTypeHelperMap(dimensionsSpec),
        in,
        new AggregatorFactory[0]
    ).getSerializedRow();

    InputRow out = InputRowSerde.fromBytes(
        InputRowSerde.getTypeHelperMap(dimensionsSpec),
        data,
        new AggregatorFactory[0]
    );

    // all nulls should be preserved
    Object rawValue = out.getRaw("d1");
    Assert.assertTrue(rawValue instanceof List);
    @SuppressWarnings("unchecked")
    List<String> rawList = (List<String>) rawValue;
    Assert.assertEquals(5, rawList.size());
    Assert.assertNull(rawList.get(0));
    Assert.assertEquals("a", rawList.get(1));
    Assert.assertNull(rawList.get(2));
    Assert.assertEquals("b", rawList.get(3));
    Assert.assertNull(rawList.get(4));
  }

  @Test
  public void testSingleNullMultiValueDimension()
  {
    // Test [null] - a single-element multi-value dimension containing only null
    HashMap<String, Object> eventWithSingleNull = new HashMap<>();
    eventWithSingleNull.put("d1", Collections.singletonList(null));

    InputRow in = new MapBasedInputRow(
        timestamp,
        ImmutableList.of("d1"),
        eventWithSingleNull
    );

    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Collections.singletonList(new StringDimensionSchema("d1"))
    );

    byte[] data = InputRowSerde.toBytes(
        InputRowSerde.getTypeHelperMap(dimensionsSpec),
        in,
        new AggregatorFactory[0]
    ).getSerializedRow();

    InputRow out = InputRowSerde.fromBytes(
        InputRowSerde.getTypeHelperMap(dimensionsSpec),
        data,
        new AggregatorFactory[0]
    );

    // [null] should be preserved as a single-element list containing null
    Object rawValue = out.getRaw("d1");
    Assert.assertNotNull(rawValue);
    Assert.assertTrue(rawValue instanceof List);
    @SuppressWarnings("unchecked")
    List<String> rawList = (List<String>) rawValue;
    Assert.assertEquals(1, rawList.size());
    Assert.assertNull(rawList.get(0));
  }

  @Test
  public void testNullStringColumn()
  {
    // Test a dimension with null value (not a list, just null)
    HashMap<String, Object> eventWithNullColumn = new HashMap<>();
    eventWithNullColumn.put("d1", null);
    eventWithNullColumn.put("d2", "valid_value");

    InputRow in = new MapBasedInputRow(
        timestamp,
        ImmutableList.of("d1", "d2"),
        eventWithNullColumn
    );

    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2")
        )
    );

    byte[] data = InputRowSerde.toBytes(
        InputRowSerde.getTypeHelperMap(dimensionsSpec),
        in,
        new AggregatorFactory[0]
    ).getSerializedRow();

    InputRow out = InputRowSerde.fromBytes(
        InputRowSerde.getTypeHelperMap(dimensionsSpec),
        data,
        new AggregatorFactory[0]
    );

    // Null column should result in empty list from getDimension()
    Assert.assertEquals(Collections.emptyList(), out.getDimension("d1"));
    // getRaw should return null for the null column
    Assert.assertNull(out.getRaw("d1"));
    // d2 should be preserved normally
    Assert.assertEquals(ImmutableList.of("valid_value"), out.getDimension("d2"));
    Assert.assertEquals("valid_value", out.getRaw("d2"));
  }

  @Test
  public void testSerdeRoundTrip()
  {
    HashMap<String, Object> event = new HashMap<>();
    event.put("d1", "single_value");
    event.put("d2", Arrays.asList("a", "b", null, "c"));
    event.put("d3", 100L);
    event.put("m1", 5.0f);

    InputRow in = new MapBasedInputRow(
        timestamp,
        ImmutableList.of("d1", "d2", "d3"),
        event
    );

    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2"),
            new LongDimensionSchema("d3")
        )
    );

    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new DoubleSumAggregatorFactory("m1out", "m1")
    };

    byte[] data = InputRowSerde.toBytes(
        InputRowSerde.getTypeHelperMap(dimensionsSpec),
        in,
        aggregatorFactories
    ).getSerializedRow();

    InputRow out = InputRowSerde.fromBytes(
        InputRowSerde.getTypeHelperMap(dimensionsSpec),
        data,
        aggregatorFactories
    );

    Assert.assertEquals(timestamp, out.getTimestampFromEpoch());
    Assert.assertEquals(ImmutableList.of("single_value"), out.getDimension("d1"));
    Assert.assertEquals(100L, out.getRaw("d3"));
    Assert.assertEquals(5.0f, out.getMetric("m1out").floatValue(), 0.00001);

    // Check multi-value dimension with null
    @SuppressWarnings("unchecked")
    List<String> d2Raw = (List<String>) out.getRaw("d2");
    Assert.assertEquals(4, d2Raw.size());
    Assert.assertEquals("a", d2Raw.get(0));
    Assert.assertEquals("b", d2Raw.get(1));
    Assert.assertNull(d2Raw.get(2));
    Assert.assertEquals("c", d2Raw.get(3));
  }
}
