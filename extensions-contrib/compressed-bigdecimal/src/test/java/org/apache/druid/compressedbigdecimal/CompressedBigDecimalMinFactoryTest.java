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

import org.apache.druid.segment.ColumnValueSelector;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;


public class CompressedBigDecimalMinFactoryTest extends CompressedBigDecimalFactoryTestBase
{
  @Test
  public void testCompressedBigDecimalMinAggregatorFactory()
  {
    CompressedBigDecimalMinAggregatorFactory aggregatorFactory = new CompressedBigDecimalMinAggregatorFactory(
        "name",
        "fieldName",
        9,
        0,
        false
    );
    Assert.assertEquals(
        "CompressedBigDecimalMinAggregatorFactory{name='name', type='COMPLEX<compressedBigDecimal>', fieldName='fieldName', requiredFields='[fieldName]', size='9', scale='0', strictNumberParsing='false'}",
        aggregatorFactory.toString()
    );
    Assert.assertNotNull(aggregatorFactory.getCacheKey());
    Assert.assertNull(aggregatorFactory.deserialize(null));
    Assert.assertEquals("5", aggregatorFactory.deserialize(new BigDecimal(5)).toString());
    Assert.assertEquals("5.0", aggregatorFactory.deserialize(5d).toString());
    Assert.assertEquals("5", aggregatorFactory.deserialize("5").toString());
    Assert.assertEquals(
        "[CompressedBigDecimalMinAggregatorFactory{name='name', type='COMPLEX<compressedBigDecimal>', fieldName='fieldName', requiredFields='[fieldName]', size='9', scale='0', strictNumberParsing='false'}]",
        Arrays.toString(aggregatorFactory.getRequiredColumns().toArray())
    );
    // default is to initialize to
    Assert.assertNull(aggregatorFactory.combine(null, null));
    Assert.assertEquals("4", aggregatorFactory.combine(new BigDecimal(4), null).toString());
    Assert.assertEquals("4", aggregatorFactory.combine(null, new BigDecimal(4)).toString());
    Assert.assertEquals(
        "4",
        aggregatorFactory.combine(
            new ArrayCompressedBigDecimal(new BigDecimal(4)),
            new ArrayCompressedBigDecimal(new BigDecimal(10))
        ).toString()
    );
  }

  @Override
  @Test
  public void testJsonSerialize() throws IOException
  {
    CompressedBigDecimalMinAggregatorFactory aggregatorFactory = new CompressedBigDecimalMinAggregatorFactory(
        "name",
        "fieldName",
        9,
        0,
        true
    );

    testJsonSerializeHelper(CompressedBigDecimalMinAggregatorFactory.class, aggregatorFactory);
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
    CompressedBigDecimalMinAggregatorFactory aggregatorFactory = new CompressedBigDecimalMinAggregatorFactory(
        "name",
        "fieldName",
        9,
        0,
        false
    );

    testCompressedBigDecimalAggregatorFactoryDeserializeHelper(aggregatorFactory);
  }

  @Override
  public void testCompressedBigDecimalBufferAggregatorGetFloat()
  {
    ColumnValueSelector<CompressedBigDecimal> valueSelector = EasyMock.createMock(ColumnValueSelector.class);
    CompressedBigDecimalMinBufferAggregator aggregator = new CompressedBigDecimalMinBufferAggregator(
        4,
        0,
        valueSelector,
        false
    );

    testCompressedBigDecimalBufferAggregatorGetFloatHelper(aggregator);
  }

  @Override
  public void testCompressedBigDecimalBufferAggregatorGetLong()
  {
    ColumnValueSelector<CompressedBigDecimal> valueSelector = EasyMock.createMock(ColumnValueSelector.class);
    CompressedBigDecimalMinBufferAggregator aggregator = new CompressedBigDecimalMinBufferAggregator(
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
    CompressedBigDecimalMinAggregateCombiner combiner = new CompressedBigDecimalMinAggregateCombiner();

    testCombinerResetHelper(combiner);
  }

  @Override
  @Test
  public void testCombinerFold()
  {
    CompressedBigDecimalMinAggregateCombiner combiner = new CompressedBigDecimalMinAggregateCombiner();

    testCombinerFoldHelper(combiner, "1", "1");
  }

  @Override
  @Test
  public void testCompressedBigDecimalAggregateCombinerGetObject()
  {
    CompressedBigDecimalMinAggregateCombiner combiner = new CompressedBigDecimalMinAggregateCombiner();

    testCompressedBigDecimalAggregateCombinerGetObjectHelper(combiner);
  }

  @Override
  public void testCompressedBigDecimalAggregateCombinerGetLong()
  {
    CompressedBigDecimalMinAggregateCombiner combiner = new CompressedBigDecimalMinAggregateCombiner();

    testCompressedBigDecimalAggregateCombinerGetLongHelper(combiner);
  }

  @Override
  public void testCompressedBigDecimalAggregateCombinerGetFloat()
  {
    CompressedBigDecimalMinAggregateCombiner combiner = new CompressedBigDecimalMinAggregateCombiner();

    testCompressedBigDecimalAggregateCombinerGetFloatHelper(combiner);
  }

  @Override
  public void testCompressedBigDecimalAggregateCombinerGetDouble()
  {
    CompressedBigDecimalMinAggregateCombiner combiner = new CompressedBigDecimalMinAggregateCombiner();

    testCompressedBigDecimalAggregateCombinerGetDoubleHelper(combiner);
  }

  @Override
  public void testCompressedBigDecimalAggregatorGetFloat()
  {
    ColumnValueSelector valueSelector = EasyMock.createMock(ColumnValueSelector.class);
    CompressedBigDecimalMinAggregator aggregator = new CompressedBigDecimalMinAggregator(2, 0, valueSelector, false);

    testCompressedBigDecimalAggregatorGetFloatHelper(aggregator);
  }

  @Override
  public void testCompressedBigDecimalAggregatorGetLong()
  {
    ColumnValueSelector valueSelector = EasyMock.createMock(ColumnValueSelector.class);
    CompressedBigDecimalMinAggregator aggregator = new CompressedBigDecimalMinAggregator(2, 0, valueSelector, false);

    testCompressedBigDecimalAggregatorGetLongHelper(aggregator);
  }

  @Override
  public void testCacheKeyEquality()
  {
    testCacheKeyEqualityHelper(CompressedBigDecimalMinAggregatorFactory::new);
  }
}
