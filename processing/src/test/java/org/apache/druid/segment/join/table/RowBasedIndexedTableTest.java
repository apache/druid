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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinTestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class RowBasedIndexedTableTest
{
  // Indexes of fields within the "countries" and "regions" tables.
  private static final int INDEX_COUNTRIES_COUNTRY_NUMBER = 0;
  private static final int INDEX_COUNTRIES_COUNTRY_ISO_CODE = 1;
  private static final int INDEX_COUNTRIES_COUNTRY_NAME = 2;
  private static final int INDEX_REGIONS_REGION_ISO_CODE = 0;

  public RowBasedIndexedTable<Map<String, Object>> countriesTable;
  public RowBasedIndexedTable<Map<String, Object>> regionsTable;

  @BeforeEach
  public void setUp() throws IOException
  {
    countriesTable = JoinTestHelper.createCountriesIndexedTable();
    regionsTable = JoinTestHelper.createRegionsIndexedTable();
  }

  @Test
  public void test_keyColumns_countries()
  {
    Assertions.assertEquals(ImmutableSet.of("countryNumber", "countryIsoCode"), countriesTable.keyColumns());
  }

  @Test
  public void test_rowSignature_countries()
  {
    Assertions.assertEquals(
        RowSignature.builder()
                    .add("countryNumber", ColumnType.LONG)
                    .add("countryIsoCode", ColumnType.STRING)
                    .add("countryName", ColumnType.STRING)
                    .build(),
        countriesTable.rowSignature()
    );
  }

  @Test
  public void test_numRows_countries()
  {
    Assertions.assertEquals(18, countriesTable.numRows());
  }

  @Test
  public void test_columnIndex_countriesCountryIsoCode()
  {
    final IndexedTable.Index index = countriesTable.columnIndex(INDEX_COUNTRIES_COUNTRY_ISO_CODE);

    Assertions.assertEquals(ImmutableSet.of(), index.find(null));
    Assertions.assertEquals(ImmutableSet.of(), index.find(2));
    Assertions.assertEquals(ImmutableSet.of(13), index.find("US"));
  }

  @Test
  public void test_columnIndex_countriesCountryNumber()
  {
    final IndexedTable.Index index = countriesTable.columnIndex(INDEX_COUNTRIES_COUNTRY_NUMBER);

    Assertions.assertEquals(ImmutableSet.of(), index.find(null));
    Assertions.assertEquals(ImmutableSet.of(0), index.find(0));
    Assertions.assertEquals(ImmutableSet.of(0), index.find(0.0));
    Assertions.assertEquals(ImmutableSet.of(0), index.find("0"));
    Assertions.assertEquals(ImmutableSet.of(2), index.find(2));
    Assertions.assertEquals(ImmutableSet.of(2), index.find(2.0));
    Assertions.assertEquals(ImmutableSet.of(2), index.find("2"));
    Assertions.assertEquals(ImmutableSet.of(), index.find(20));
    Assertions.assertEquals(ImmutableSet.of(), index.find("US"));
  }

  @Test
  public void test_columnIndex_countriesCountryName()
  {
    Exception e = Assertions.assertThrows(
        Exception.class,
        () -> countriesTable.columnIndex(INDEX_COUNTRIES_COUNTRY_NAME)
    );
    Assertions.assertTrue(e.getMessage().contains("Column[2] is not a key column"));
  }

  @Test
  public void test_columnIndex_countriesOutOfBounds()
  {
    Assertions.assertThrows(IndexOutOfBoundsException.class, () -> countriesTable.columnIndex(99));
  }

  @Test
  public void test_columnIndex_regionsRegionIsoCode()
  {
    final IndexedTable.Index index = regionsTable.columnIndex(INDEX_REGIONS_REGION_ISO_CODE);

    Assertions.assertEquals(ImmutableSet.of(21), index.find(null));
    Assertions.assertEquals(ImmutableSet.of(0), index.find("11"));
    Assertions.assertEquals(ImmutableSet.of(1), index.find(13));
    Assertions.assertEquals(ImmutableSet.of(12), index.find("QC"));
    Assertions.assertEquals(ImmutableSet.of(15, 16), index.find("VA"));
  }

  @Test
  public void test_columnReader_countriesCountryNumber()
  {
    final IndexedTable.Reader reader = countriesTable.columnReader(INDEX_COUNTRIES_COUNTRY_NUMBER);

    Assertions.assertEquals(0L, reader.read(0));
    Assertions.assertEquals(1L, reader.read(1));
  }

  @Test
  public void test_columnReader_countriesCountryName()
  {
    final IndexedTable.Reader reader = countriesTable.columnReader(INDEX_COUNTRIES_COUNTRY_NAME);

    Assertions.assertEquals("Australia", reader.read(0));
    Assertions.assertEquals("Canada", reader.read(1));
    Assertions.assertEquals("Atlantis", reader.read(14));
  }

  @Test
  public void test_columnReader_countriesOutOfBoundsRow()
  {
    final IndexedTable.Reader reader = countriesTable.columnReader(INDEX_COUNTRIES_COUNTRY_NUMBER);
    Assertions.assertThrows(IndexOutOfBoundsException.class, () -> reader.read(99));
  }

  @Test
  public void test_columnReader_countriesOutOfBoundsColumn()
  {
    Assertions.assertThrows(IndexOutOfBoundsException.class, () -> countriesTable.columnReader(99));
  }

  @Test
  public void testVersion()
  {
    Assertions.assertEquals(JoinTestHelper.INDEXED_TABLE_VERSION, countriesTable.version());
    Assertions.assertEquals(JoinTestHelper.INDEXED_TABLE_VERSION, regionsTable.version());
  }

  @Test
  public void testIsCacheable() throws IOException
  {
    Assertions.assertFalse(countriesTable.isCacheable());
    RowBasedIndexedTable<Map<String, Object>> countriesTableWithCacheKey = JoinTestHelper.createCountriesIndexedTableWithCacheKey();
    Assertions.assertTrue(countriesTableWithCacheKey.isCacheable());
    Assertions.assertArrayEquals(JoinTestHelper.INDEXED_TABLE_CACHE_KEY, countriesTableWithCacheKey.computeCacheKey());
  }
}
