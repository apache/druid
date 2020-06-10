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
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.VSizeColumnarInts;
import org.apache.druid.segment.data.VSizeColumnarMultiInts;
import org.apache.druid.segment.serde.DictionaryEncodedColumnSupplier;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ValueMatchersTest extends InitializedNullHandlingTest
{
  private DictionaryEncodedColumnSupplier supplierSingleConstant;
  private DictionaryEncodedColumnSupplier supplierSingle;
  private DictionaryEncodedColumnSupplier supplierMulti;

  @Before
  public void setup()
  {
    supplierSingleConstant = new DictionaryEncodedColumnSupplier(
        GenericIndexed.fromIterable(ImmutableList.of("value"), GenericIndexed.STRING_STRATEGY),
        () -> VSizeColumnarInts.fromArray(new int[]{0}),
        null,
        0
    );
    supplierSingle = new DictionaryEncodedColumnSupplier(
        GenericIndexed.fromIterable(ImmutableList.of("value", "value2"), GenericIndexed.STRING_STRATEGY),
        () -> VSizeColumnarInts.fromArray(new int[]{0, 0, 1, 0, 1}),
        null,
        0
    );
    supplierMulti = new DictionaryEncodedColumnSupplier(
        GenericIndexed.fromIterable(ImmutableList.of("value"), GenericIndexed.STRING_STRATEGY),
        null,
        () -> VSizeColumnarMultiInts.fromIterable(
            ImmutableList.of(
                VSizeColumnarInts.fromArray(new int[]{0, 0}),
                VSizeColumnarInts.fromArray(new int[]{0})
            )
        ),
        0
    );
  }
  @Test
  public void testNullDimensionSelectorCanBeBoolean()
  {
    Boolean resultMatchNull = ValueMatchers.toBooleanIfPossible(
        DimensionSelector.constant(null),
        false,
        string -> string == null
    );
    Assert.assertNotNull(resultMatchNull);
    Assert.assertTrue(resultMatchNull);

    Boolean resultMatchNotNull = ValueMatchers.toBooleanIfPossible(
        DimensionSelector.constant(null),
        false,
        string -> string != null
    );
    Assert.assertNotNull(resultMatchNotNull);
    Assert.assertFalse(resultMatchNotNull);

    Boolean resultMatchNonNilConstant = ValueMatchers.toBooleanIfPossible(
        supplierSingleConstant.get().makeDimensionSelector(new SimpleAscendingOffset(1), null),
        false,
        string -> string != null
    );
    Assert.assertNotNull(resultMatchNonNilConstant);
    Assert.assertTrue(resultMatchNonNilConstant);

    Boolean resultMatchNonNil = ValueMatchers.toBooleanIfPossible(
        supplierSingle.get().makeDimensionSelector(new SimpleAscendingOffset(1), null),
        false,
        string -> string != null
    );
    Assert.assertNull(resultMatchNonNil);

    Boolean resultMatchNonNilMulti = ValueMatchers.toBooleanIfPossible(
        supplierMulti.get().makeDimensionSelector(new SimpleAscendingOffset(1), null),
        true,
        string -> string != null
    );
    Assert.assertNull(resultMatchNonNilMulti);
  }

  @Test
  public void testNilVectorSelectorCanBeBoolean()
  {
    Boolean resultMatchNull = ValueMatchers.toBooleanIfPossible(
        NilVectorSelector.create(new NoFilterVectorOffset(10, 0, 100)),
        false,
        string -> string == null
    );
    Assert.assertNotNull(resultMatchNull);
    Assert.assertTrue(resultMatchNull);

    Boolean resultMatchNotNull = ValueMatchers.toBooleanIfPossible(
        NilVectorSelector.create(new NoFilterVectorOffset(10, 0, 100)),
        false,
        string -> string != null
    );
    Assert.assertNotNull(resultMatchNotNull);
    Assert.assertFalse(resultMatchNotNull);

    Boolean resultMatchNotNilConstant = ValueMatchers.toBooleanIfPossible(
        supplierSingleConstant.get().makeSingleValueDimensionVectorSelector(new NoFilterVectorOffset(10, 0, 1)),
        false,
        string -> string != null
    );
    Assert.assertNotNull(resultMatchNotNilConstant);
    Assert.assertTrue(resultMatchNotNilConstant);

    Boolean resultMatchNotNil = ValueMatchers.toBooleanIfPossible(
        supplierSingle.get().makeSingleValueDimensionVectorSelector(new NoFilterVectorOffset(10, 0, 1)),
        false,
        string -> string != null
    );
    Assert.assertNull(resultMatchNotNil);

    Boolean resultMatchNotNilMulti = ValueMatchers.toBooleanIfPossible(
        supplierMulti.get().makeSingleValueDimensionVectorSelector(new NoFilterVectorOffset(10, 0, 1)),
        true,
        string -> string != null
    );
    Assert.assertNull(resultMatchNotNilMulti);
  }
}
