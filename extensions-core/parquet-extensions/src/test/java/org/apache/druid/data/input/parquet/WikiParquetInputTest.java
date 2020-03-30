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
public class WikiParquetInputTest extends BaseParquetInputTest
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

  public WikiParquetInputTest(String parserType) throws IOException
  {
    this.parserType = parserType;
    this.job = Job.getInstance(new Configuration());
  }

  @Test
  public void testWiki() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/wiki/wiki.json",
        parserType,
        false
    );
    config.intoConfiguration(job);

    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    Assert.assertEquals("Gypsy Danger", rows.get(0).getDimension("page").get(0));
    String s1 = rows.get(0).getDimension("language").get(0);
    String s2 = rows.get(0).getDimension("language").get(1);
    Assert.assertEquals("en", s1);
    Assert.assertEquals("zh", s2);
  }
}
