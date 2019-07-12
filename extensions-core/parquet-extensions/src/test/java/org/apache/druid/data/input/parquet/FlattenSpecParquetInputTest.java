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
public class FlattenSpecParquetInputTest extends BaseParquetInputTest
{
  private static final String TS1 = "2018-09-18T00:18:00.023Z";

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

  public FlattenSpecParquetInputTest(String parserType) throws IOException
  {
    this.parserType = parserType;
    this.job = Job.getInstance(new Configuration());
  }

  @Test
  public void testFlat1NoFlattenSpec() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/flattening/flat_1.json",
        parserType,
        false
    );
    config.intoConfiguration(job);
    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    Assert.assertEquals(TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    Assert.assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    Assert.assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    Assert.assertEquals("listDim1v1", rows.get(0).getDimension("listDim").get(0));
    Assert.assertEquals("listDim1v2", rows.get(0).getDimension("listDim").get(1));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testFlat1Autodiscover() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/flattening/flat_1_autodiscover_fields.json",
        parserType,
        true
    );
    config.intoConfiguration(job);
    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    Assert.assertEquals(TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    Assert.assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    Assert.assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    Assert.assertEquals("listDim1v1", rows.get(0).getDimension("listDim").get(0));
    Assert.assertEquals("listDim1v2", rows.get(0).getDimension("listDim").get(1));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testFlat1Flatten() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/flattening/flat_1_flatten.json",
        parserType,
        true
    );
    config.intoConfiguration(job);
    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    Assert.assertEquals(TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    Assert.assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    Assert.assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    Assert.assertEquals("listDim1v1", rows.get(0).getDimension("list").get(0));
    Assert.assertEquals("listDim1v2", rows.get(0).getDimension("list").get(1));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testFlat1FlattenSelectListItem() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/flattening/flat_1_list_index.json",
        parserType,
        true
    );
    config.intoConfiguration(job);
    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    Assert.assertEquals(TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    Assert.assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    Assert.assertEquals("listDim1v2", rows.get(0).getDimension("listextracted").get(0));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }


  @Test
  public void testNested1NoFlattenSpec() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/flattening/nested_1.json",
        parserType,
        false
    );
    config.intoConfiguration(job);
    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    Assert.assertEquals(TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    List<String> dims = rows.get(0).getDimensions();
    Assert.assertFalse(dims.contains("dim2"));
    Assert.assertFalse(dims.contains("dim3"));
    Assert.assertFalse(dims.contains("listDim"));
    Assert.assertFalse(dims.contains("nestedData"));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testNested1Autodiscover() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/flattening/nested_1_autodiscover_fields.json",
        parserType,
        true
    );
    config.intoConfiguration(job);
    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    Assert.assertEquals(TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    List<String> dims = rows.get(0).getDimensions();
    Assert.assertFalse(dims.contains("dim2"));
    Assert.assertFalse(dims.contains("dim3"));
    Assert.assertFalse(dims.contains("listDim"));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testNested1Flatten() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/flattening/nested_1_flatten.json",
        parserType,
        true
    );
    config.intoConfiguration(job);
    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    Assert.assertEquals(TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    Assert.assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    Assert.assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    Assert.assertEquals("listDim1v1", rows.get(0).getDimension("listDim").get(0));
    Assert.assertEquals("listDim1v2", rows.get(0).getDimension("listDim").get(1));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
    Assert.assertEquals(2, rows.get(0).getMetric("metric2").longValue());
  }

  @Test
  public void testNested1FlattenSelectListItem() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/flattening/nested_1_list_index.json",
        parserType,
        true
    );
    config.intoConfiguration(job);
    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    Assert.assertEquals(TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    Assert.assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    Assert.assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    Assert.assertEquals("listDim1v2", rows.get(0).getDimension("listextracted").get(0));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }
}
