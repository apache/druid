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

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.selector.TestColumnValueSelector;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * common tests for {@link AggregatorFactory} implementations
 */
public abstract class CompressedBigDecimalFactoryTestBase
{
  private static final Object FLAG = new Object();

  @Test
  public abstract void testJsonSerialize() throws IOException;

  @Test
  public abstract void testFinalizeComputation();

  @Test
  public abstract void testCompressedBigDecimalAggregatorFactoryDeserialize();

  @Test
  public abstract void testCombinerReset();

  @Test
  public abstract void testCombinerFold();

  @Test
  public abstract void testCompressedBigDecimalAggregateCombinerGetObject();

  @Test(expected = UnsupportedOperationException.class)
  public abstract void testCompressedBigDecimalAggregateCombinerGetLong();

  @Test(expected = UnsupportedOperationException.class)
  public abstract void testCompressedBigDecimalAggregateCombinerGetFloat();

  @Test(expected = UnsupportedOperationException.class)
  public abstract void testCompressedBigDecimalAggregateCombinerGetDouble();

  @Test(expected = UnsupportedOperationException.class)
  public abstract void testCompressedBigDecimalAggregatorGetFloat();

  @Test(expected = UnsupportedOperationException.class)
  public abstract void testCompressedBigDecimalAggregatorGetLong();

  @Test(expected = UnsupportedOperationException.class)
  public abstract void testCompressedBigDecimalBufferAggregatorGetFloat();

  @Test(expected = UnsupportedOperationException.class)
  public abstract void testCompressedBigDecimalBufferAggregatorGetLong();

  @Test
  public abstract void testCacheKeyEquality();

  protected <T> void testJsonSerializeHelper(Class<T> clazz, T aggregatorFactory) throws IOException
  {
    ObjectMapper objectMapper = new ObjectMapper();

    objectMapper.disable(
        MapperFeature.AUTO_DETECT_CREATORS,
        MapperFeature.AUTO_DETECT_FIELDS,
        MapperFeature.AUTO_DETECT_GETTERS,
        MapperFeature.AUTO_DETECT_IS_GETTERS
    );

    String jsonString = objectMapper.writeValueAsString(aggregatorFactory);
    T deserializedAggregatorFactory = objectMapper.readValue(jsonString, clazz);

    Assert.assertEquals(aggregatorFactory, deserializedAggregatorFactory);
  }

  @SuppressWarnings("ConstantConditions")
  protected void testFinalizeComputationHelper(AggregatorFactory aggregatorFactory)
  {
    ArrayCompressedBigDecimal result1 = (ArrayCompressedBigDecimal) aggregatorFactory.finalizeComputation(
        new ArrayCompressedBigDecimal(new BigDecimal("100.3141592"))
    );

    Assert.assertEquals("100.3141592", result1.toString());

    ArrayCompressedBigDecimal result2 = (ArrayCompressedBigDecimal) aggregatorFactory.finalizeComputation(
        new ArrayCompressedBigDecimal(new BigDecimal("0.000000000"))
    );

    Assert.assertEquals("0", result2.toString());

    Object result3 = aggregatorFactory.finalizeComputation(null);

    Assert.assertNull(result3);

    ArrayCompressedBigDecimal result4 = (ArrayCompressedBigDecimal) aggregatorFactory.finalizeComputation(
        new ArrayCompressedBigDecimal(new BigDecimal("1.000000000"))
    );

    Assert.assertEquals("1.000000000", result4.toString());

  }

  protected void testCompressedBigDecimalAggregatorFactoryDeserializeHelper(AggregatorFactory aggregatorFactory)
  {
    CompressedBigDecimal compressedBigDecimal = (CompressedBigDecimal) aggregatorFactory.deserialize(5);
    Assert.assertEquals("5", compressedBigDecimal.toString());
  }

  protected void testCompressedBigDecimalBufferAggregatorGetFloatHelper(BufferAggregator aggregator)
  {
    ByteBuffer byteBuffer = ByteBuffer.allocate(10);
    aggregator.getFloat(byteBuffer, 0);
  }

  protected void testCompressedBigDecimalBufferAggregatorGetLongHelper(BufferAggregator aggregator)
  {
    ByteBuffer byteBuffer = ByteBuffer.allocate(10);
    aggregator.getLong(byteBuffer, 0);
  }

  protected <T> void testCombinerResetHelper(AggregateCombiner<T> combiner)
  {
    TestColumnValueSelector<CompressedBigDecimal> columnValueSelector = TestColumnValueSelector.of(
        CompressedBigDecimal.class,
        ImmutableList.of(ArrayCompressedBigDecimal.wrap(new int[]{67, 0}, 0)),
        DateTimes.of("2020-01-01")
    );

    columnValueSelector.advance();
    combiner.reset(columnValueSelector);
    Assert.assertEquals("67", combiner.getObject().toString());
  }

  protected <T> void testCombinerFoldHelper(AggregateCombiner<T> combiner, String result1, String result2)
  {
    TestColumnValueSelector<CompressedBigDecimal> columnValueSelector = TestColumnValueSelector.of(
        CompressedBigDecimal.class,
        ImmutableList.of(
            ArrayCompressedBigDecimal.wrap(new int[]{1, 0}, 0),
            ArrayCompressedBigDecimal.wrap(new int[]{10, 0}, 0)
        ),
        DateTimes.of("2020-01-01")
    );

    columnValueSelector.advance();
    combiner.fold(columnValueSelector);
    Assert.assertEquals(result1, combiner.getObject().toString());
    columnValueSelector.advance();
    combiner.fold(columnValueSelector);
    Assert.assertEquals(result2, combiner.getObject().toString());
  }

  protected <T> void testCompressedBigDecimalAggregateCombinerGetObjectHelper(AggregateCombiner<T> combiner)
  {
    T compressedBigDecimal = combiner.getObject();
    Assert.assertSame(null, compressedBigDecimal);
  }

  protected <T> void testCompressedBigDecimalAggregateCombinerGetLongHelper(AggregateCombiner<T> combiner)
  {
    combiner.getLong();
  }

  protected <T> void testCompressedBigDecimalAggregateCombinerGetFloatHelper(AggregateCombiner<T> combiner)
  {
    combiner.getFloat();
  }

  protected <T> void testCompressedBigDecimalAggregateCombinerGetDoubleHelper(AggregateCombiner<T> combiner)
  {
    combiner.getDouble();
  }

  protected void testCompressedBigDecimalAggregatorGetFloatHelper(Aggregator aggregator)
  {
    aggregator.getFloat();
  }

  protected void testCompressedBigDecimalAggregatorGetLongHelper(Aggregator aggregator)
  {
    aggregator.getLong();
  }

  /**
   * creates a series of pairs of instances that should not be unique and verifies. Also tests when cache keys should
   * be the same
   */
  protected void testCacheKeyEqualityHelper(CompressedBigDecimalAggregatorFactoryCreator factoryCreator)
  {
    Assert.assertEquals(
        Base64.getEncoder().encodeToString(
            factoryCreator.create(
                "name1",
                "fieldName1",
                10,
                3,
                true
            ).getCacheKey()
        ),
        Base64.getEncoder().encodeToString(
            factoryCreator.create(
                "name2",
                "fieldName1",
                10,
                3,
                true
            ).getCacheKey()
        )
    );
    Assert.assertNotEquals(
        Base64.getEncoder().encodeToString(
            factoryCreator.create(
                "name1",
                "fieldName1",
                10,
                3,
                true
            ).getCacheKey()
        ),
        Base64.getEncoder().encodeToString(
            factoryCreator.create(
                "name1",
                "fieldName2",
                10,
                3,
                true
            ).getCacheKey()
        )
    );
    Assert.assertNotEquals(
        Base64.getEncoder().encodeToString(
            factoryCreator.create(
                "name1",
                "fieldName1",
                10,
                3,
                true
            ).getCacheKey()
        ),
        Base64.getEncoder().encodeToString(
            factoryCreator.create(
                "name1",
                "fieldName1",
                6,
                3,
                true
            ).getCacheKey()
        )
    );
    Assert.assertNotEquals(
        Base64.getEncoder().encodeToString(
            factoryCreator.create(
                "name1",
                "fieldName1",
                10,
                3,
                true
            ).getCacheKey()
        ),
        Base64.getEncoder().encodeToString(
            factoryCreator.create(
                "name1",
                "fieldName1",
                10,
                9,
                true
            ).getCacheKey()
        )
    );
    Assert.assertNotEquals(
        Base64.getEncoder().encodeToString(
            factoryCreator.create(
                "name1",
                "fieldName1",
                10,
                3,
                true
            ).getCacheKey()
        ),
        Base64.getEncoder().encodeToString(
            factoryCreator.create(
                "name1",
                "fieldName1",
                10,
                3,
                false
            ).getCacheKey()
        )
    );

  }
}
