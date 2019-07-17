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

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

@RunWith(Parameterized.class)
public class DecimalParquetInputTest extends BaseParquetInputTest
{
  @Parameterized.Parameters(name = "type = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE},
        new Object[]{ParquetExtensionsModule.PARQUET_SIMPLE_INPUT_PARSER_TYPE}
    );
  }

  private final String parserType;

  public DecimalParquetInputTest(String parserType) throws IOException
  {
    this.parserType = parserType;
  }

  @Test
  public void testReadParquetDecimalFixedLen() throws IOException, InterruptedException
  {
    // parquet-avro does not correctly convert decimal types
    if (parserType.equals(ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE)) {
      return;
    }
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/decimals/dec_in_fix_len.json",
        parserType,
        true
    );
    List<InputRow> rows = getAllRows(parserType, config);
    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    Assert.assertEquals("1.0", rows.get(0).getDimension("fixed_len_dec").get(0));
    Assert.assertEquals(new BigDecimal("1.0"), rows.get(0).getMetric("metric1"));
  }

  @Test
  public void testReadParquetDecimali32() throws IOException, InterruptedException
  {
    // parquet-avro does not correctly convert decimal types
    if (parserType.equals(ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE)) {
      return;
    }
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/decimals/dec_in_i32.json",
        parserType,
        true
    );
    List<InputRow> rows = getAllRows(parserType, config);
    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    Assert.assertEquals("100", rows.get(0).getDimension("i32_dec").get(0));
    Assert.assertEquals(new BigDecimal(100), rows.get(0).getMetric("metric1"));
  }

  @Test
  public void testReadParquetDecimali64() throws IOException, InterruptedException
  {
    // parquet-avro does not correctly convert decimal types
    if (parserType.equals(ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE)) {
      return;
    }
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/decimals/dec_in_i64.json",
        parserType,
        true
    );
    List<InputRow> rows = getAllRows(parserType, config);
    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    Assert.assertEquals("100", rows.get(0).getDimension("i64_dec").get(0));
    Assert.assertEquals(new BigDecimal(100), rows.get(0).getMetric("metric1"));
  }
}
