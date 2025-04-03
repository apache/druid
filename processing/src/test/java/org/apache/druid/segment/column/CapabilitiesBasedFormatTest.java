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

import org.apache.druid.java.util.common.ISE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CapabilitiesBasedFormatTest
{
  @Test
  public void testMerge()
  {
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                  .setHasMultipleValues(true);
    ColumnCapabilities capabilities2 = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                   .setDictionaryEncoded(true)
                                                                   .setDictionaryValuesSorted(true)
                                                                   .setDictionaryValuesUnique(true)
                                                                   .setHasBitmapIndexes(true)
                                                                   .setHasNulls(true);

    CapabilitiesBasedFormat format = new CapabilitiesBasedFormat(capabilities);


    // merged format should pick up combined capabilities, just check the resulting merged capabilities since they
    // drive everything in CapabilitiesBasedFormat
    ColumnCapabilities mergedCapabilities = format.merge(new CapabilitiesBasedFormat(capabilities2))
                                                  .toColumnCapabilities();
    Assertions.assertTrue(mergedCapabilities.is(ValueType.STRING));
    // false if either is false
    Assertions.assertFalse(mergedCapabilities.hasBitmapIndexes());
    // also false if either is false, but these don't have any known implications for segment creation since they are
    // computed when the segment is loaded and only used at query time
    Assertions.assertFalse(mergedCapabilities.areDictionaryValuesSorted().isTrue());
    Assertions.assertFalse(mergedCapabilities.areDictionaryValuesUnique().isTrue());
    // rest are true if either is true
    Assertions.assertTrue(mergedCapabilities.isDictionaryEncoded().isTrue());
    Assertions.assertTrue(mergedCapabilities.hasMultipleValues().isTrue());
    Assertions.assertTrue(mergedCapabilities.hasNulls().isTrue());
  }

  @Test
  public void testMergeNullCapabilities()
  {
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                  .setHasMultipleValues(true);

    CapabilitiesBasedFormat format = new CapabilitiesBasedFormat(capabilities);


    ColumnFormat merged = format.merge(null);
    // same object since other is null
    Assertions.assertEquals(format, merged);
  }

  @Test
  public void testMergeIncompatibleArray()
  {
    ColumnCapabilities arrayString = ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ColumnType.STRING_ARRAY);
    ColumnCapabilities arrayLong = ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ColumnType.LONG_ARRAY);
    ColumnCapabilities string = ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities();

    ColumnFormat stringArrayFormat = new CapabilitiesBasedFormat(arrayString);
    ColumnFormat longArrayFormat = new CapabilitiesBasedFormat(arrayLong);
    CapabilitiesBasedFormat stringFormat = new CapabilitiesBasedFormat(string);

    Throwable t = Assertions.assertThrows(
        ISE.class,
        () -> stringArrayFormat.merge(longArrayFormat)
    );
    Assertions.assertEquals("Cannot merge columns of type[ARRAY<STRING>] and [ARRAY<LONG>]", t.getMessage());


    t = Assertions.assertThrows(
        ISE.class,
        () -> stringArrayFormat.merge(stringFormat)
    );
    Assertions.assertEquals("Cannot merge columns of type[ARRAY<STRING>] and [STRING]", t.getMessage());
  }

  @Test
  public void testMergeIncompatibleComplexType()
  {
    ColumnCapabilities complex1 = new ColumnCapabilitiesImpl().setType(ColumnType.ofComplex("someComplex"));
    ColumnCapabilities complex2 = new ColumnCapabilitiesImpl().setType(ColumnType.ofComplex("otherComplex"));
    ColumnCapabilities string = ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities();


    ColumnFormat format1 = new CapabilitiesBasedFormat(complex1);
    ColumnFormat format2 = new CapabilitiesBasedFormat(complex2);
    CapabilitiesBasedFormat stringFormat = new CapabilitiesBasedFormat(string);

    Throwable t = Assertions.assertThrows(
        ISE.class,
        () -> format1.merge(format2)
    );
    Assertions.assertEquals("Cannot merge columns of type[COMPLEX<someComplex>] and [COMPLEX<otherComplex>]", t.getMessage());


    t = Assertions.assertThrows(
        ISE.class,
        () -> format1.merge(stringFormat)
    );
    Assertions.assertEquals("Cannot merge columns of type[COMPLEX<someComplex>] and [STRING]", t.getMessage());
  }
}
