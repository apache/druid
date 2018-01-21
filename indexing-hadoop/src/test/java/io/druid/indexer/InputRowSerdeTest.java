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
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
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
import io.druid.common.config.NullHandling;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

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

  public InputRowSerdeTest()
  {
    this.timestamp = System.currentTimeMillis();
    this.dims = ImmutableList.of("dim_non_existing", "d1", "d2");
    this.event = ImmutableMap.<String, Object>of(
        "d1", "d1v",
        "d2", ImmutableList.of("d2v1", "d2v2"),
        "m1", 5.0f,
        "m2", 100L,
        "m3", "m3v"
    );
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

    byte[] data = InputRowSerde.toBytes(in, aggregatorFactories, false); // Ignore Unparseable aggregator
    InputRow out = InputRowSerde.fromBytes(data, aggregatorFactories);

    Assert.assertEquals(timestamp, out.getTimestampFromEpoch());
    Assert.assertEquals(dims, out.getDimensions());
    Assert.assertEquals(Collections.EMPTY_LIST, out.getDimension("dim_non_existing"));
    Assert.assertEquals(ImmutableList.of("d1v"), out.getDimension("d1"));
    Assert.assertEquals(ImmutableList.of("d2v1", "d2v2"), out.getDimension("d2"));

    if (NullHandling.useDefaultValuesForNull()) {
      Assert.assertEquals(0.0f, out.getMetric("agg_non_existing").floatValue(), 0.00001);
    } else {
      Assert.assertNull(out.getMetric("agg_non_existing"));
    }

    Assert.assertEquals(0L, out.getMetric("unparseable"));
    Assert.assertEquals(5.0f, out.getMetric("m1out").floatValue(), 0.00001);
    Assert.assertEquals(100L, out.getMetric("m2out"));
    Assert.assertEquals(1, ((HyperLogLogCollector) out.getRaw("m3out")).estimateCardinality(), 0.001);

    EasyMock.verify(mockedAggregator);
    EasyMock.verify(mockedNullAggregator);
  }

  @Test(expected = ParseException.class)
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

    InputRowSerde.toBytes(in, aggregatorFactories, true);

  }
}
