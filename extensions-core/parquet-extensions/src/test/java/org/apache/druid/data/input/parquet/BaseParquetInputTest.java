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
import org.apache.directory.api.util.Strings;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.path.StaticPathSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class BaseParquetInputTest
{
  private static Map<String, String> parseSpecType = ImmutableMap.of(
      ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE,
      ParquetExtensionsModule.PARQUET_AVRO_PARSE_SPEC_TYPE,
      ParquetExtensionsModule.PARQUET_SIMPLE_INPUT_PARSER_TYPE,
      ParquetExtensionsModule.PARQUET_SIMPLE_PARSE_SPEC_TYPE
  );

  private static Map<String, String> inputFormatType = ImmutableMap.of(
      ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE,
      "org.apache.druid.data.input.parquet.DruidParquetAvroInputFormat",
      ParquetExtensionsModule.PARQUET_SIMPLE_INPUT_PARSER_TYPE,
      "org.apache.druid.data.input.parquet.DruidParquetInputFormat"
  );

  private static Map<String, Class<? extends InputFormat>> inputFormatClass = ImmutableMap.of(
      ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE,
      DruidParquetAvroInputFormat.class,
      ParquetExtensionsModule.PARQUET_SIMPLE_INPUT_PARSER_TYPE,
      DruidParquetInputFormat.class
  );

  static HadoopDruidIndexerConfig transformHadoopDruidIndexerConfig(
      String templateFile,
      String type,
      boolean withParseType
  )
      throws IOException
  {
    String template = Strings.utf8ToString(Files.readAllBytes(Paths.get(templateFile)));
    String transformed;
    if (withParseType) {
      transformed = StringUtils.format(template, inputFormatType.get(type), type, parseSpecType.get(type));
    } else {
      transformed = StringUtils.format(template, inputFormatType.get(type), type);
    }
    return HadoopDruidIndexerConfig.fromString(transformed);
  }


  static Object getFirstRow(Job job, String parserType, String parquetPath) throws IOException, InterruptedException
  {
    File testFile = new File(parquetPath);
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(
        inputFormatClass.get(parserType),
        job.getConfiguration()
    );
    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());

    try (RecordReader reader = inputFormat.createRecordReader(split, context)) {

      reader.initialize(split, context);
      reader.nextKeyValue();
      return reader.getCurrentValue();
    }
  }

  static List<InputRow> getAllRows(String parserType, HadoopDruidIndexerConfig config)
      throws IOException, InterruptedException
  {
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    File testFile = new File(((StaticPathSpec) config.getPathSpec()).getPaths());
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(
        inputFormatClass.get(parserType),
        job.getConfiguration()
    );
    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());

    try (RecordReader reader = inputFormat.createRecordReader(split, context)) {
      List<InputRow> records = new ArrayList<>();
      InputRowParser parser = config.getParser();

      reader.initialize(split, context);
      while (reader.nextKeyValue()) {
        reader.nextKeyValue();
        Object data = reader.getCurrentValue();
        records.add(((List<InputRow>) parser.parseBatch(data)).get(0));
      }

      return records;
    }
  }
}
