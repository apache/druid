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

package org.apache.druid.data.input.parquet;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("ALL")
public class ParquetToJsonTest
{
  @ClassRule
  public static TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void testSanity() throws Exception
  {
    final File tmpDir = tmp.newFolder();
    try (InputStream in = new BufferedInputStream(ClassLoader.getSystemResourceAsStream("smlTbl.parquet"))) {
      Files.copy(in, tmpDir.toPath().resolve("smlTbl.parquet"));
    }

    ParquetToJson.main(new String[]{tmpDir.toString()});

    DefaultObjectMapper mapper = DefaultObjectMapper.INSTANCE;
    List<Object> objs = mapper.readerFor(Object.class).readValues(new File(tmpDir, "smlTbl.parquet.json")).readAll();

    Assert.assertEquals(56, objs.size());
    Assert.assertEquals(
        ImmutableMap
            .builder()
            .put("col_int", 8122)
            .put("col_bgint", 817200)
            .put("col_char_2", "IN")
            .put("col_vchar_52", "AXXXXXXXXXXXXXXXXXXXXXXXXXCXXXXXXXXXXXXXXXXXXXXXXXXB")
            .put("col_tmstmp", 1409617682418L)
            .put("col_dt", 422717616000000L)
            .put("col_booln", false)
            .put("col_dbl", 12900.48)
            .put("col_tm", 33109170)
            .build(),
        objs.get(0)
    );
  }

  @Test
  public void testConvertedDates() throws Exception
  {
    final File tmpDir = tmp.newFolder();
    try (InputStream in = new BufferedInputStream(ClassLoader.getSystemResourceAsStream("smlTbl.parquet"))) {
      Files.copy(in, tmpDir.toPath().resolve("smlTbl.parquet"));
    }

    ParquetToJson.main(new String[]{"--convert-corrupt-dates", tmpDir.toString()});

    DefaultObjectMapper mapper = DefaultObjectMapper.INSTANCE;
    List<Object> objs = mapper.readerFor(Object.class).readValues(new File(tmpDir, "smlTbl.parquet.json")).readAll();

    Assert.assertEquals(56, objs.size());
    Assert.assertEquals(
        ImmutableMap
            .builder()
            .put("col_int", 8122)
            .put("col_bgint", 817200)
            .put("col_char_2", "IN")
            .put("col_vchar_52", "AXXXXXXXXXXXXXXXXXXXXXXXXXCXXXXXXXXXXXXXXXXXXXXXXXXB")
            .put("col_tmstmp", 1409617682418L)
            .put("col_dt", 984009600000L)
            .put("col_booln", false)
            .put("col_dbl", 12900.48)
            .put("col_tm", 33109170)
            .build(),
        objs.get(0)
    );
  }


  @Test
  public void testInputValidation()
  {
    Assert.assertThrows(IAE.class, () -> ParquetToJson.main(new String[]{}));
    Assert.assertThrows(IAE.class, () -> ParquetToJson.main(new String[]{"a", "b"}));
  }

  @Test
  public void testEmptyDir() throws Exception
  {
    final File tmpDir = tmp.newFolder();
    Assert.assertThrows(IAE.class, () -> ParquetToJson.main(new String[] {tmpDir.getAbsolutePath()}));
  }

  @Test
  public void testSomeFile() throws Exception
  {
    final File file = tmp.newFile();
    assertTrue(file.exists());
    Assert.assertThrows(IAE.class, () -> ParquetToJson.main(new String[] {file.getAbsolutePath()}));
  }

  @Test
  public void testNonExistentFile() throws Exception
  {
    final File file = new File(tmp.getRoot(), "nonExistent");
    assertFalse(file.exists());
    Assert.assertThrows(IAE.class, () -> ParquetToJson.main(new String[] {file.getAbsolutePath()}));
  }
}
