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

package org.apache.druid.segment.filter;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.VSizeColumnarInts;
import org.apache.druid.segment.data.VSizeColumnarMultiInts;
import org.apache.druid.segment.serde.StringUtf8DictionaryEncodedColumnSupplier;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ValueMatchersTest extends InitializedNullHandlingTest
{
  private StringUtf8DictionaryEncodedColumnSupplier<?> supplierSingleConstant;
  private StringUtf8DictionaryEncodedColumnSupplier<?> supplierSingle;
  private StringUtf8DictionaryEncodedColumnSupplier<?> supplierMulti;

  @Before
  public void setup()
  {
    supplierSingleConstant = new StringUtf8DictionaryEncodedColumnSupplier<>(
        GenericIndexed.fromIterable(
            ImmutableList.of(ByteBuffer.wrap(StringUtils.toUtf8("value"))),
            GenericIndexed.UTF8_STRATEGY
        )::singleThreaded,
        () -> VSizeColumnarInts.fromArray(new int[]{0}),
        null
    );
    supplierSingle = new StringUtf8DictionaryEncodedColumnSupplier<>(
        GenericIndexed.fromIterable(
            ImmutableList.of(
                ByteBuffer.wrap(StringUtils.toUtf8("value")),
                ByteBuffer.wrap(StringUtils.toUtf8("value2"))
            ),
            GenericIndexed.UTF8_STRATEGY
        )::singleThreaded,
        () -> VSizeColumnarInts.fromArray(new int[]{0, 0, 1, 0, 1}),
        null
    );
    supplierMulti = new StringUtf8DictionaryEncodedColumnSupplier<>(
        GenericIndexed.fromIterable(
            ImmutableList.of(ByteBuffer.wrap(StringUtils.toUtf8("value"))),
            GenericIndexed.UTF8_STRATEGY
        )::singleThreaded,
        null,
        () -> VSizeColumnarMultiInts.fromIterable(
            ImmutableList.of(
                VSizeColumnarInts.fromArray(new int[]{0, 0}),
                VSizeColumnarInts.fromArray(new int[]{0})
            )
        )
    );
  }
  @Test
  public void testNullDimensionSelectorCanBeBoolean()
  {
    ConstantMatcherType resultMatchNull = ValueMatchers.toConstantMatcherTypeIfPossible(
        DimensionSelector.constant(null),
        false,
        DruidObjectPredicate.isNull()
    );
    Assert.assertNotNull(resultMatchNull);
    Assert.assertEquals(ConstantMatcherType.ALL_TRUE, resultMatchNull);

    ConstantMatcherType resultMatchNotNull = ValueMatchers.toConstantMatcherTypeIfPossible(
        DimensionSelector.constant(null),
        false,
        DruidObjectPredicate.notNull()
    );
    Assert.assertNotNull(resultMatchNotNull);
    Assert.assertEquals(ConstantMatcherType.ALL_FALSE, resultMatchNotNull);

    ConstantMatcherType resultMatchNullUnknown = ValueMatchers.toConstantMatcherTypeIfPossible(
        DimensionSelector.constant(null),
        false,
        value -> value == null ? DruidPredicateMatch.UNKNOWN : DruidPredicateMatch.of(true)
    );
    Assert.assertNotNull(resultMatchNullUnknown);
    Assert.assertEquals(ConstantMatcherType.ALL_UNKNOWN, resultMatchNullUnknown);

    ConstantMatcherType resultMatchNonNilConstant = ValueMatchers.toConstantMatcherTypeIfPossible(
        supplierSingleConstant.get().makeDimensionSelector(new SimpleAscendingOffset(1), null),
        false,
        DruidObjectPredicate.notNull()
    );
    Assert.assertNotNull(resultMatchNonNilConstant);
    Assert.assertEquals(ConstantMatcherType.ALL_TRUE, resultMatchNonNilConstant);

    ConstantMatcherType resultMatchNonNil = ValueMatchers.toConstantMatcherTypeIfPossible(
        supplierSingle.get().makeDimensionSelector(new SimpleAscendingOffset(1), null),
        false,
        DruidObjectPredicate.notNull()
    );
    Assert.assertNull(resultMatchNonNil);

    ConstantMatcherType resultMatchNonNilMulti = ValueMatchers.toConstantMatcherTypeIfPossible(
        supplierMulti.get().makeDimensionSelector(new SimpleAscendingOffset(1), null),
        true,
        DruidObjectPredicate.notNull()
    );
    Assert.assertNull(resultMatchNonNilMulti);
  }

  @Test
  public void testNilVectorSelectorCanBeBoolean()
  {
    ConstantMatcherType resultMatchNull = ValueMatchers.toConstantMatcherTypeIfPossible(
        NilVectorSelector.create(new NoFilterVectorOffset(10, 0, 100)),
        false,
        DruidObjectPredicate.isNull()
    );
    Assert.assertNotNull(resultMatchNull);
    Assert.assertEquals(ConstantMatcherType.ALL_TRUE, resultMatchNull);

    ConstantMatcherType resultMatchNotNull = ValueMatchers.toConstantMatcherTypeIfPossible(
        NilVectorSelector.create(new NoFilterVectorOffset(10, 0, 100)),
        false,
        DruidObjectPredicate.notNull()
    );
    Assert.assertNotNull(resultMatchNotNull);
    Assert.assertEquals(ConstantMatcherType.ALL_FALSE, resultMatchNotNull);

    ConstantMatcherType resultMatchNullUnknown = ValueMatchers.toConstantMatcherTypeIfPossible(
        NilVectorSelector.create(new NoFilterVectorOffset(10, 0, 100)),
        false,
        value -> value == null ? DruidPredicateMatch.UNKNOWN : DruidPredicateMatch.of(true)
    );
    Assert.assertNotNull(resultMatchNullUnknown);
    Assert.assertEquals(ConstantMatcherType.ALL_UNKNOWN, resultMatchNullUnknown);

    ConstantMatcherType resultMatchNotNilConstant = ValueMatchers.toConstantMatcherTypeIfPossible(
        supplierSingleConstant.get().makeSingleValueDimensionVectorSelector(new NoFilterVectorOffset(10, 0, 1)),
        false,
        DruidObjectPredicate.notNull()
    );
    Assert.assertNotNull(resultMatchNotNilConstant);
    Assert.assertEquals(ConstantMatcherType.ALL_TRUE, resultMatchNotNilConstant);

    ConstantMatcherType resultMatchNotNil = ValueMatchers.toConstantMatcherTypeIfPossible(
        supplierSingle.get().makeSingleValueDimensionVectorSelector(new NoFilterVectorOffset(10, 0, 1)),
        false,
        DruidObjectPredicate.notNull()
    );
    Assert.assertNull(resultMatchNotNil);

    ConstantMatcherType resultMatchNotNilMulti = ValueMatchers.toConstantMatcherTypeIfPossible(
        supplierMulti.get().makeSingleValueDimensionVectorSelector(new NoFilterVectorOffset(10, 0, 1)),
        true,
        DruidObjectPredicate.notNull()
    );
    Assert.assertNull(resultMatchNotNilMulti);
  }
}
