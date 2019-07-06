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
import org.apache.druid.indexer.path.StaticPathSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.List;

@RunWith(Parameterized.class)
public class TimestampsParquetInputTest extends BaseParquetInputTest
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
  private final Job job;

  public TimestampsParquetInputTest(String parserType) throws IOException
  {
    this.parserType = parserType;
    this.job = Job.getInstance(new Configuration());
  }

  @Test
  public void testDateHandling() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig configTimeAsString = transformHadoopDruidIndexerConfig(
        "example/timestamps/date_test_data_job_string.json",
        parserType,
        false
    );
    HadoopDruidIndexerConfig configTimeAsDate = transformHadoopDruidIndexerConfig(
        "example/timestamps/date_test_data_job_date.json",
        parserType,
        false
    );
    List<InputRow> rowsWithString = getAllRows(parserType, configTimeAsString);
    List<InputRow> rowsWithDate = getAllRows(parserType, configTimeAsDate);
    Assert.assertEquals(rowsWithDate.size(), rowsWithString.size());

    for (int i = 0; i < rowsWithDate.size(); i++) {
      Assert.assertEquals(rowsWithString.get(i).getTimestamp(), rowsWithDate.get(i).getTimestamp());
    }
  }

  @Test
  public void testParseInt96Timestamp() throws IOException, InterruptedException
  {
    // parquet-avro does not support int96, but if it ever does, remove this
    if (parserType.equals(ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE)) {
      return;
    }

    // the source parquet file was found in apache spark sql repo tests, where it is known as impala_timestamp.parq
    // it has a single column, "ts" which is an int96 timestamp
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/timestamps/int96_timestamp.json",
        parserType,
        true
    );
    config.intoConfiguration(job);
    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    Assert.assertEquals("2001-01-01T01:01:01.000Z", rows.get(0).getTimestamp().toString());
  }

  @Test
  public void testTimeMillisInInt64() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/timestamps/timemillis_in_i64.json",
        parserType,
        true
    );
    config.intoConfiguration(job);
    List<InputRow> rows = getAllRows(parserType, config);
    Assert.assertEquals("1970-01-01T00:00:00.010Z", rows.get(0).getTimestamp().toString());
  }
}
