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
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;


public class AggregatorCombinerFactoryTest
{
  /**
   * Test method for {@link CompressedBigDecimalColumn}.
   */
  @Test
  public void testCompressedBigDecimalColumn()
  {
    ColumnarMultiInts cmi = EasyMock.createMock(ColumnarMultiInts.class);
    ColumnarInts ci = EasyMock.createMock(ColumnarInts.class);
    ReadableOffset ro = EasyMock.createMock(ReadableOffset.class);
    CompressedBigDecimalColumn cbr = new CompressedBigDecimalColumn(ci, cmi);
    Assert.assertEquals(CompressedBigDecimalModule.COMPRESSED_BIG_DECIMAL, cbr.getTypeName());
    Assert.assertEquals(0, cbr.getLength());
    Assert.assertEquals(CompressedBigDecimalColumn.class, cbr.getClazz());
    Assert.assertNotNull(cbr.makeColumnValueSelector(ro));
  }

  /**
   * Test method for {@link CompressedBigDecimalAggregatorFactory}.
   */
  @Test
  public void testCompressedBigDecimalAggregatorFactory()
  {
    CompressedBigDecimalAggregatorFactory cf = new CompressedBigDecimalAggregatorFactory(
        "name",
        "fieldName",
        9,
        0,
        false
    );
    Assert.assertEquals(
        "CompressedBigDecimalSumAggregatorFactory{name='name', type='compressedBigDecimal', fieldName='fieldName', requiredFields='[fieldName]', size='9', scale='0', strictNumberParsing='false'}",
        cf.toString()
    );
    Assert.assertNotNull(cf.getCacheKey());
    Assert.assertNull(cf.deserialize(null));
    Assert.assertEquals("5", cf.deserialize(new BigDecimal(5)).toString());
    Assert.assertEquals("5", cf.deserialize(5d).toString());
    Assert.assertEquals("5", cf.deserialize("5").toString());
    Assert.assertEquals(
        "[CompressedBigDecimalSumAggregatorFactory{name='fieldName', type='compressedBigDecimal', fieldName='fieldName', requiredFields='[fieldName]', size='9', scale='0', strictNumberParsing='false'}]",
        Arrays.toString(cf.getRequiredColumns().toArray())
    );
    Assert.assertEquals("0", cf.combine(null, null).toString());
    Assert.assertEquals("4", cf.combine(new BigDecimal(4), null).toString());
    Assert.assertEquals("4", cf.combine(null, new BigDecimal(4)).toString());
    Assert.assertEquals(
        "8",
        cf.combine(
            new ArrayCompressedBigDecimal(new BigDecimal(4)),
            new ArrayCompressedBigDecimal(new BigDecimal(4))
        ).toString()
    );
  }

  /**
   * Test method for {@link CompressedBigDecimalAggregatorFactory#deserialize(Object)}.
   */
  @Test(expected = RuntimeException.class)
  public void testCompressedBigDecimalAggregatorFactoryDeserialize()
  {
    CompressedBigDecimalAggregatorFactory cf = new CompressedBigDecimalAggregatorFactory(
        "name",
        "fieldName",
        9,
        0,
        false
    );
    cf.deserialize(5);
  }

  /**
   * Test method for {@link CompressedBigDecimalBufferAggregator#getFloat(ByteBuffer, int)}
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCompressedBigDecimalBufferAggregatorGetFloat()
  {
    ColumnValueSelector<CompressedBigDecimal> cs = EasyMock.createMock(ColumnValueSelector.class);
    ByteBuffer bbuf = ByteBuffer.allocate(10);
    CompressedBigDecimalBufferAggregator ca = new CompressedBigDecimalBufferAggregator(4, 0, cs, false);
    ca.getFloat(bbuf, 0);
  }

  /**
   * Test method for {@link CompressedBigDecimalBufferAggregator#getLong(ByteBuffer, int)}
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCompressedBigDecimalBufferAggregatorGetLong()
  {
    ColumnValueSelector<CompressedBigDecimal> cs = EasyMock.createMock(ColumnValueSelector.class);
    ByteBuffer bbuf = ByteBuffer.allocate(10);
    CompressedBigDecimalBufferAggregator ca = new CompressedBigDecimalBufferAggregator(4, 0, cs, false);
    ca.getLong(bbuf, 0);
  }

  /**
   * Test method for {@link CompressedBigDecimalAggregateCombiner#getObject()}
   */
  @Test
  public void testCompressedBigDecimalAggregateCombinerGetObject()
  {
    CompressedBigDecimalAggregateCombiner cc = new CompressedBigDecimalAggregateCombiner();
    CompressedBigDecimal c = cc.getObject();
    Assert.assertSame(null, c);
  }

  /**
   * Test method for {@link CompressedBigDecimalAggregateCombiner#getClass()}
   */
  @Test
  public void testCompressedBigDecimalAggregateCombinerClassofObject()
  {
    CompressedBigDecimalAggregateCombiner cc = new CompressedBigDecimalAggregateCombiner();
    Assert.assertSame(CompressedBigDecimalAggregateCombiner.class, cc.getClass());
  }

  /**
   * Test method for {@link CompressedBigDecimalAggregateCombiner#getLong()}
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCompressedBigDecimalAggregateCombinerGetLong()
  {
    CompressedBigDecimalAggregateCombiner cc = new CompressedBigDecimalAggregateCombiner();
    cc.getLong();
  }

  /**
   * Test method for {@link CompressedBigDecimalAggregateCombiner#getFloat()}
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCompressedBigDecimalAggregateCombinerGetFloat()
  {
    CompressedBigDecimalAggregateCombiner cc = new CompressedBigDecimalAggregateCombiner();
    cc.getFloat();
  }

  /**
   * Test method for {@link CompressedBigDecimalAggregateCombiner#getDouble()}
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCompressedBigDecimalAggregateCombinerGetDouble()
  {
    CompressedBigDecimalAggregateCombiner cc = new CompressedBigDecimalAggregateCombiner();
    cc.getDouble();
  }

  /**
   * Test method for {@link CompressedBigDecimalAggregator#getFloat()}
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCompressedBigDecimalAggregatorGetFloat()
  {
    ColumnValueSelector cv = EasyMock.createMock(ColumnValueSelector.class);
    CompressedBigDecimalAggregator cc = new CompressedBigDecimalAggregator(2, 0, cv, false);
    cc.getFloat();
  }

  /**
   * Test method for {@link CompressedBigDecimalAggregator#getLong()}
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCompressedBigDecimalAggregatorGetLong()
  {
    ColumnValueSelector cv = EasyMock.createMock(ColumnValueSelector.class);
    CompressedBigDecimalAggregator cc = new CompressedBigDecimalAggregator(2, 0, cv, false);
    cc.getLong();
  }
}
