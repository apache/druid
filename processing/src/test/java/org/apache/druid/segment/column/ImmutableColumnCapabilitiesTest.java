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

package org.apache.druid.segment.column;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ImmutableColumnCapabilitiesTest
{
  @Test
  public void testInternedOfCopiesCapabilities()
  {
    final ColumnCapabilitiesImpl capabilities = ColumnCapabilitiesImpl.createDefault()
                                                                      .setType(ColumnType.STRING_ARRAY)
                                                                      .setDictionaryEncoded(true)
                                                                      .setDictionaryValuesSorted(true)
                                                                      .setHasMultipleValues(false)
                                                                      .setHasBitmapIndexes(true)
                                                                      .setHasSpatialIndexes(true)
                                                                      .setHasNulls(ColumnCapabilities.Capable.UNKNOWN);
    final ImmutableColumnCapabilities copy = ImmutableColumnCapabilities.internedOf(capabilities);

    Assertions.assertEquals(ColumnType.STRING_ARRAY, copy.toColumnType());
    Assertions.assertEquals(ColumnCapabilities.Capable.TRUE, copy.isDictionaryEncoded());
    Assertions.assertEquals(ColumnCapabilities.Capable.TRUE, copy.areDictionaryValuesSorted());
    Assertions.assertEquals(ColumnCapabilities.Capable.FALSE, copy.areDictionaryValuesUnique());
    Assertions.assertEquals(ColumnCapabilities.Capable.FALSE, copy.hasMultipleValues());
    Assertions.assertTrue(copy.hasBitmapIndexes());
    Assertions.assertTrue(copy.hasSpatialIndexes());
    Assertions.assertEquals(ColumnCapabilities.Capable.UNKNOWN, copy.hasNulls());

    // later changes to the source don't affect the copy
    capabilities.setHasNulls(true);
    Assertions.assertEquals(ColumnCapabilities.Capable.UNKNOWN, copy.hasNulls());
  }

  @Test
  public void testInternedOfInterns()
  {
    final ImmutableColumnCapabilities a =
        ImmutableColumnCapabilities.internedOf(ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG));
    final ImmutableColumnCapabilities b =
        ImmutableColumnCapabilities.internedOf(ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG));
    final ImmutableColumnCapabilities c =
        ImmutableColumnCapabilities.internedOf(ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE));

    Assertions.assertSame(a, b);
    Assertions.assertSame(a, ImmutableColumnCapabilities.internedOf(a));
    Assertions.assertNotEquals(a, c);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ImmutableColumnCapabilities.class).usingGetClass().verify();
  }
}
