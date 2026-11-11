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

package org.apache.druid.compressedbigdecimal;

import org.apache.druid.compressedbigdecimal.aggregator.max.CompressedBigDecimalMaxAggregatorFactory;
import org.apache.druid.compressedbigdecimal.aggregator.sum.CompressedBigDecimalSumAggregateCombiner;
import org.apache.druid.compressedbigdecimal.aggregator.sum.CompressedBigDecimalSumAggregator;
import org.apache.druid.compressedbigdecimal.aggregator.sum.CompressedBigDecimalSumAggregatorFactory;
import org.apache.druid.compressedbigdecimal.aggregator.sum.CompressedBigDecimalSumBufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * test CompressedBigDecimalSumFactory and various aggregators and combiner produced
 */
public class CompressedBigDecimalSumFactoryTest extends CompressedBigDecimalFactoryTestBase
{
  @Test
  public void testCompressedBigDecimalAggregatorFactory()
  {
    CompressedBigDecimalSumAggregatorFactory aggregatorFactory = new CompressedBigDecimalSumAggregatorFactory(
        "name",
        "fieldName",
        9,
        0,
        false
    );
    Assertions.assertEquals(
        "CompressedBigDecimalSumAggregatorFactory{name='name', type='COMPLEX<compressedBigDecimal>', fieldName='fieldName', requiredFields='[fieldName]', size='9', scale='0', strictNumberParsing='false'}",
        aggregatorFactory.toString()
    );
    Assertions.assertNotNull(aggregatorFactory.getCacheKey());
    Assertions.assertNull(aggregatorFactory.deserialize(null));
    Assertions.assertEquals("5", aggregatorFactory.deserialize(new BigDecimal(5)).toString());
    Assertions.assertEquals("5.0", aggregatorFactory.deserialize(5d).toString());
    Assertions.assertEquals("5", aggregatorFactory.deserialize("5").toString());

    Assertions.assertEquals("0", aggregatorFactory.combine(null, null).toString());
    Assertions.assertEquals("4", aggregatorFactory.combine(new BigDecimal(4), null).toString());
    Assertions.assertEquals("4", aggregatorFactory.combine(null, new BigDecimal(4)).toString());
    Assertions.assertEquals(
        "8",
        aggregatorFactory.combine(
            new ArrayCompressedBigDecimal(new BigDecimal(4)),
            new ArrayCompressedBigDecimal(new BigDecimal(4))
        ).toString()
    );
  }

  @Override
  @Test
  public void testJsonSerialize() throws IOException
  {
    CompressedBigDecimalSumAggregatorFactory aggregatorFactory = new CompressedBigDecimalSumAggregatorFactory(
        "name",
        "fieldName",
        9,
        0,
        true
    );

    testJsonSerializeHelper(CompressedBigDecimalSumAggregatorFactory.class, aggregatorFactory);
  }

  @Override
  @Test
  public void testFinalizeComputation()
  {
    CompressedBigDecimalMaxAggregatorFactory aggregatorFactory = new CompressedBigDecimalMaxAggregatorFactory(
        "name",
        "fieldName",
        9,
        0,
        false
    );

    testFinalizeComputationHelper(aggregatorFactory);
  }

  @Override
  @Test
  public void testCompressedBigDecimalAggregatorFactoryDeserialize()
  {
    CompressedBigDecimalSumAggregatorFactory aggregatorFactory = new CompressedBigDecimalSumAggregatorFactory(
        "name",
        "fieldName",
        9,
        0,
        false
    );

    testCompressedBigDecimalAggregatorFactoryDeserializeHelper(aggregatorFactory);
  }

  @Override
  @Test
  public void testCompressedBigDecimalBufferAggregatorGetFloat()
  {
    ColumnValueSelector<CompressedBigDecimal> columnValueSelector = EasyMock.createMock(ColumnValueSelector.class);
    CompressedBigDecimalSumBufferAggregator aggregator = new CompressedBigDecimalSumBufferAggregator(
        4,
        0,
        columnValueSelector,
        false
    );

    testCompressedBigDecimalBufferAggregatorGetFloatHelper(aggregator);
  }

  @Override
  @Test
  public void testCompressedBigDecimalBufferAggregatorGetLong()
  {
    ColumnValueSelector<CompressedBigDecimal> valueSelector = EasyMock.createMock(ColumnValueSelector.class);
    CompressedBigDecimalSumBufferAggregator aggregator = new CompressedBigDecimalSumBufferAggregator(
        4,
        0,
        valueSelector,
        false
    );

    testCompressedBigDecimalBufferAggregatorGetLongHelper(aggregator);
  }

  @Override
  @Test
  public void testCombinerReset()
  {
    CompressedBigDecimalSumAggregateCombiner combiner = new CompressedBigDecimalSumAggregateCombiner();

    testCombinerResetHelper(combiner);
  }

  @Override
  @Test
  public void testCombinerFold()
  {
    CompressedBigDecimalSumAggregateCombiner combiner = new CompressedBigDecimalSumAggregateCombiner();

    testCombinerFoldHelper(combiner, "1", "11");
  }

  @Override
  @Test
  public void testCompressedBigDecimalAggregateCombinerGetObject()
  {
    CompressedBigDecimalSumAggregateCombiner combiner = new CompressedBigDecimalSumAggregateCombiner();

    testCompressedBigDecimalAggregateCombinerGetObjectHelper(combiner);
  }

  @Override
  @Test
  public void testCompressedBigDecimalAggregateCombinerGetLong()
  {
    CompressedBigDecimalSumAggregateCombiner combiner = new CompressedBigDecimalSumAggregateCombiner();

    testCompressedBigDecimalAggregateCombinerGetLongHelper(combiner);
  }

  @Override
  @Test
  public void testCompressedBigDecimalAggregateCombinerGetFloat()
  {
    CompressedBigDecimalSumAggregateCombiner combiner = new CompressedBigDecimalSumAggregateCombiner();

    testCompressedBigDecimalAggregateCombinerGetFloatHelper(combiner);
  }

  @Override
  @Test
  public void testCompressedBigDecimalAggregateCombinerGetDouble()
  {
    CompressedBigDecimalSumAggregateCombiner combiner = new CompressedBigDecimalSumAggregateCombiner();

    testCompressedBigDecimalAggregateCombinerGetDoubleHelper(combiner);
  }

  @Override
  @Test
  public void testCompressedBigDecimalAggregatorGetFloat()
  {
    ColumnValueSelector valueSelector = EasyMock.createMock(ColumnValueSelector.class);
    CompressedBigDecimalSumAggregator aggregator = new CompressedBigDecimalSumAggregator(2, 0, valueSelector, false);

    testCompressedBigDecimalAggregatorGetFloatHelper(aggregator);
  }

  @Override
  @Test
  public void testCompressedBigDecimalAggregatorGetLong()
  {
    ColumnValueSelector valueSelector = EasyMock.createMock(ColumnValueSelector.class);
    CompressedBigDecimalSumAggregator aggregator = new CompressedBigDecimalSumAggregator(2, 0, valueSelector, false);

    testCompressedBigDecimalAggregatorGetLongHelper(aggregator);
  }

  @Override
  @Test
  public void testCacheKeyEquality()
  {
    testCacheKeyEqualityHelper(CompressedBigDecimalSumAggregatorFactory::new);
  }
}
