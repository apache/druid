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

package org.apache.druid.frame.key;

import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

public class RowKeyTest extends InitializedNullHandlingTest
{
  @Test
  public void testEqualsAndHashCode()
  {
    // Can't use EqualsVerifier because it requires that cached hashcodes be computed in the constructor.
    final RowSignature signatureLong = RowSignature.builder().add("1", ColumnType.LONG).build();
    final RowSignature signatureLongString =
        RowSignature.builder().add("1", ColumnType.LONG).add("2", ColumnType.STRING).build();

    //noinspection AssertBetweenInconvertibleTypes: testing this case on purpose
    Assert.assertNotEquals(
        "not a key",
        KeyTestUtils.createKey(signatureLong, 1L)
    );

    Assert.assertEquals(
        KeyTestUtils.createKey(signatureLong, 1L),
        KeyTestUtils.createKey(signatureLong, 1L)
    );

    Assert.assertEquals(
        KeyTestUtils.createKey(RowSignature.empty()),
        KeyTestUtils.createKey(RowSignature.empty())
    );

    Assert.assertNotEquals(
        KeyTestUtils.createKey(RowSignature.empty()),
        KeyTestUtils.createKey(signatureLong, 2L)
    );

    Assert.assertNotEquals(
        KeyTestUtils.createKey(signatureLong, 1L),
        KeyTestUtils.createKey(signatureLong, 2L)
    );

    Assert.assertEquals(
        KeyTestUtils.createKey(signatureLongString, 1L, "abc"),
        KeyTestUtils.createKey(signatureLongString, 1L, "abc")
    );

    Assert.assertNotEquals(
        KeyTestUtils.createKey(signatureLongString, 1L, "abc"),
        KeyTestUtils.createKey(signatureLongString, 1L, "def")
    );

    Assert.assertEquals(
        KeyTestUtils.createKey(signatureLong, 1L).hashCode(),
        KeyTestUtils.createKey(signatureLong, 1L).hashCode()
    );

    Assert.assertNotEquals(
        KeyTestUtils.createKey(signatureLong, 1L).hashCode(),
        KeyTestUtils.createKey(signatureLong, 2L).hashCode()
    );

    Assert.assertEquals(
        KeyTestUtils.createKey(signatureLongString, 1L, "abc").hashCode(),
        KeyTestUtils.createKey(signatureLongString, 1L, "abc").hashCode()
    );

    Assert.assertNotEquals(
        KeyTestUtils.createKey(signatureLongString, 1L, "abc").hashCode(),
        KeyTestUtils.createKey(signatureLongString, 1L, "def").hashCode()
    );
  }

  @Test
  public void testGetNumberOfBytes()
  {
    final RowSignature signatureLong = RowSignature.builder().add("1", ColumnType.LONG).build();
    final RowKey longKey = KeyTestUtils.createKey(signatureLong, 1L, "abc");
    Assert.assertEquals(longKey.array().length, longKey.getNumberOfBytes());

    final RowSignature signatureLongString =
        RowSignature.builder().add("1", ColumnType.LONG).add("2", ColumnType.STRING).build();
    final RowKey longStringKey = KeyTestUtils.createKey(signatureLongString, 1L, "abc");
    Assert.assertEquals(longStringKey.array().length, longStringKey.getNumberOfBytes());
  }
}
