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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.builder.CliBuilder;
import org.apache.druid.data.input.parquet.simple.ParquetGroupConverter;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Converts parquet files into new-deliminated JSON object files. Takes a single
 * argument (an input directory) and processes all files that end with a
 * ".parquet" extension. Writes out a new file in the same directory named by
 * appending ".json" to the old file name. Will overwrite any output file that
 * already exists.
 */
@Command(name = "ParquetToJson")
public class ParquetToJson implements Callable<Void>
{

  @Option(name = "--convert-corrupt-dates")
  public boolean convertCorruptDates = false;

  @Arguments(description = "directory")
  public List<String> directories;


  public static void main(String[] args) throws Exception
  {
    CliBuilder<Callable> builder = Cli.builder("ParquetToJson");
    builder.withDefaultCommand(ParquetToJson.class);
    builder.build().parse(args).call();
  }

  private File[] getInputFiles()
  {
    if (directories == null || directories.size() != 1) {
      throw new IAE("Only one directory argument is supported!");
    }

    File dir = new File(directories.get(0));
    if (!dir.isDirectory()) {
      throw new IAE("Not a directory [%s]", dir);
    }
    File[] inputFiles = dir.listFiles(
        pathname -> pathname.getName().endsWith(".parquet"));
    if (inputFiles == null || inputFiles.length == 0) {
      throw new IAE("No parquet files in directory [%s]", dir);
    }
    return inputFiles;
  }

  @Override
  public Void call() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    File[] inputFiles = getInputFiles();

    for (File inputFile : inputFiles) {
      File outputFile = new File(inputFile.getAbsolutePath() + ".json");

      try (
          final org.apache.parquet.hadoop.ParquetReader<Group> reader = org.apache.parquet.hadoop.ParquetReader
              .builder(new GroupReadSupport(), new Path(inputFile.toURI()))
              .build();
          final SequenceWriter writer = mapper.writer().withRootValueSeparator("\n").writeValues(outputFile)) {
        ParquetGroupConverter converter = new ParquetGroupConverter(true, convertCorruptDates);
        Group group;
        while ((group = reader.read()) != null) {
          writer.write(converter.convertGroup(group));
        }
      }
    }
    return null;
  }
}
