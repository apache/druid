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

package org.apache.druid.segment.join.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.JoinTestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Map;

public class RowBasedIndexedTableTest
{
  // Indexes of fields within the "countries" and "regions" tables.
  private static final int INDEX_COUNTRIES_COUNTRY_NUMBER = 0;
  private static final int INDEX_COUNTRIES_COUNTRY_ISO_CODE = 1;
  private static final int INDEX_COUNTRIES_COUNTRY_NAME = 2;
  private static final int INDEX_REGIONS_REGION_ISO_CODE = 0;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public RowBasedIndexedTable<Map<String, Object>> countriesTable;
  public RowBasedIndexedTable<Map<String, Object>> regionsTable;

  @BeforeClass
  public static void setUpStatic()
  {
    NullHandling.initializeForTests();
  }

  @Before
  public void setUp() throws IOException
  {
    countriesTable = JoinTestHelper.createCountriesIndexedTable();
    regionsTable = JoinTestHelper.createRegionsIndexedTable();
  }

  @Test
  public void test_keyColumns_countries()
  {
    Assert.assertEquals(ImmutableSet.of("countryNumber", "countryIsoCode"), countriesTable.keyColumns());
  }

  @Test
  public void test_rowSignature_countries()
  {
    Assert.assertEquals(
        RowSignature.builder()
                    .add("countryNumber", ValueType.LONG)
                    .add("countryIsoCode", ValueType.STRING)
                    .add("countryName", ValueType.STRING)
                    .build(),
        countriesTable.rowSignature()
    );
  }

  @Test
  public void test_numRows_countries()
  {
    Assert.assertEquals(18, countriesTable.numRows());
  }

  @Test
  public void test_columnIndex_countriesCountryIsoCode()
  {
    final IndexedTable.Index index = countriesTable.columnIndex(INDEX_COUNTRIES_COUNTRY_ISO_CODE);

    Assert.assertEquals(ImmutableList.of(), index.find(null));
    Assert.assertEquals(ImmutableList.of(), index.find(2));
    Assert.assertEquals(ImmutableList.of(13), index.find("US"));
  }

  @Test
  public void test_columnIndex_countriesCountryNumber()
  {
    final IndexedTable.Index index = countriesTable.columnIndex(INDEX_COUNTRIES_COUNTRY_NUMBER);

    Assert.assertEquals(ImmutableList.of(), index.find(null));
    Assert.assertEquals(ImmutableList.of(0), index.find(0));
    Assert.assertEquals(ImmutableList.of(0), index.find(0.0));
    Assert.assertEquals(ImmutableList.of(0), index.find("0"));
    Assert.assertEquals(ImmutableList.of(2), index.find(2));
    Assert.assertEquals(ImmutableList.of(2), index.find(2.0));
    Assert.assertEquals(ImmutableList.of(2), index.find("2"));
    Assert.assertEquals(ImmutableList.of(), index.find(20));
    Assert.assertEquals(ImmutableList.of(), index.find("US"));
  }

  @Test
  public void test_columnIndex_countriesCountryName()
  {
    expectedException.expectMessage("Column[2] is not a key column");
    countriesTable.columnIndex(INDEX_COUNTRIES_COUNTRY_NAME);
  }

  @Test
  public void test_columnIndex_countriesOutOfBounds()
  {
    expectedException.expect(IndexOutOfBoundsException.class);
    countriesTable.columnIndex(99);
  }

  @Test
  public void test_columnIndex_regionsRegionIsoCode()
  {
    final IndexedTable.Index index = regionsTable.columnIndex(INDEX_REGIONS_REGION_ISO_CODE);

    Assert.assertEquals(ImmutableList.of(), index.find(null));
    Assert.assertEquals(ImmutableList.of(0), index.find("11"));
    Assert.assertEquals(ImmutableList.of(1), index.find(13));
    Assert.assertEquals(ImmutableList.of(12), index.find("QC"));
    Assert.assertEquals(ImmutableList.of(15, 16), index.find("VA"));
  }

  @Test
  public void test_columnReader_countriesCountryNumber()
  {
    final IndexedTable.Reader reader = countriesTable.columnReader(INDEX_COUNTRIES_COUNTRY_NUMBER);

    Assert.assertEquals(0L, reader.read(0));
    Assert.assertEquals(1L, reader.read(1));
  }

  @Test
  public void test_columnReader_countriesCountryName()
  {
    final IndexedTable.Reader reader = countriesTable.columnReader(INDEX_COUNTRIES_COUNTRY_NAME);

    Assert.assertEquals("Australia", reader.read(0));
    Assert.assertEquals("Canada", reader.read(1));
    Assert.assertEquals("Atlantis", reader.read(14));
  }

  @Test
  public void test_columnReader_countriesOutOfBoundsRow()
  {
    final IndexedTable.Reader reader = countriesTable.columnReader(INDEX_COUNTRIES_COUNTRY_NUMBER);
    expectedException.expect(IndexOutOfBoundsException.class);
    reader.read(99);
  }

  @Test
  public void test_columnReader_countriesOutOfBoundsColumn()
  {
    expectedException.expect(IndexOutOfBoundsException.class);
    countriesTable.columnReader(99);
  }

  @Test
  public void testVersion()
  {
    Assert.assertEquals(JoinTestHelper.INDEXED_TABLE_VERSION, countriesTable.version());
    Assert.assertEquals(JoinTestHelper.INDEXED_TABLE_VERSION, regionsTable.version());
  }
}
