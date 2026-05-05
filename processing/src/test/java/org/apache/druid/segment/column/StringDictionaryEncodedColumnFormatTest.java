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

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.segment.StringColumnFormatSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringDictionaryEncodedColumnFormatTest
{
  private static final StringColumnFormatSpec SPEC = StringColumnFormatSpec.builder()
      .setMaxStringLength(50)
      .build();

  @Test
  public void testGetColumnSchemaWithSpec()
  {
    StringDictionaryEncodedColumnFormat format = new StringDictionaryEncodedColumnFormat(
        false, false, true, false, SPEC
    );
    DimensionSchema schema = format.getColumnSchema("city");
    StringDimensionSchema stringSchema = (StringDimensionSchema) schema;
    Assertions.assertNotNull(stringSchema.getColumnFormatSpec());
    Assertions.assertEquals(Integer.valueOf(50), stringSchema.getColumnFormatSpec().getMaxStringLength());
  }

  @Test
  public void testMergeTwoFormatsKeepsSpec()
  {
    StringDictionaryEncodedColumnFormat formatWithSpec = new StringDictionaryEncodedColumnFormat(
        false, false, true, false, SPEC
    );
    StringDictionaryEncodedColumnFormat formatWithoutSpec = new StringDictionaryEncodedColumnFormat(
        false, true, true, false, null
    );
    ColumnFormat merged = formatWithSpec.merge(formatWithoutSpec);

    DimensionSchema schema = merged.getColumnSchema("city");
    Assertions.assertEquals(Integer.valueOf(50), ((StringDimensionSchema) schema).getColumnFormatSpec().getMaxStringLength());
  }

  @Test
  public void testCapabilitiesBasedFormatMergesDelegatesToStringFormat()
  {
    StringDictionaryEncodedColumnFormat formatWithSpec = new StringDictionaryEncodedColumnFormat(
        false, false, true, false, SPEC
    );
    ColumnCapabilities caps = ColumnCapabilitiesImpl.createDefault()
        .setType(ColumnType.STRING)
        .setDictionaryEncoded(true)
        .setDictionaryValuesSorted(true)
        .setDictionaryValuesUnique(true)
        .setHasMultipleValues(false)
        .setHasNulls(false)
        .setHasBitmapIndexes(true);
    CapabilitiesBasedFormat capFormat = new CapabilitiesBasedFormat(caps);

    ColumnFormat merged = capFormat.merge(formatWithSpec);
    Assertions.assertInstanceOf(StringDictionaryEncodedColumnFormat.class, merged);
    DimensionSchema schema = merged.getColumnSchema("city");
    Assertions.assertNotNull(((StringDimensionSchema) schema).getColumnFormatSpec());
  }
}
