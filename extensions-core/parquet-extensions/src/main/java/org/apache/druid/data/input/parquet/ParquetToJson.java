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
import org.apache.druid.data.input.parquet.simple.ParquetGroupConverter;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.File;

/**
 * Converts parquet files into new-deliminated JSON object files.  Takes a single argument (an input directory)
 * and processes all files that end with a ".parquet" extension.  Writes out a new file in the same directory named
 * by appending ".json" to the old file name.  Will overwrite any output file that already exists.
 */
public class ParquetToJson
{

  public static void main(String[] args) throws Exception
  {
    if (args.length != 1) {
      throw new IAE("Usage: directory");
    }

    ParquetGroupConverter converter = new ParquetGroupConverter(true);
    ObjectMapper mapper = new DefaultObjectMapper();

    File dir = new File(args[0]);
    if (!dir.isDirectory()) {
      throw new IAE("Not a directory [%s]", args[0]);
    }
    File[] inputFiles = dir.listFiles(
        pathname -> pathname.getName().endsWith(".parquet")
    );
    if (inputFiles == null || inputFiles.length == 0) {
      throw new IAE("No parquet files in directory [%s]", args[0]);
    }
    for (File inputFile : inputFiles) {
      File outputFile = new File(inputFile.getAbsolutePath() + ".json");

      try (
          final org.apache.parquet.hadoop.ParquetReader<Group> reader = org.apache.parquet.hadoop.ParquetReader
              .builder(new GroupReadSupport(), new Path(inputFile.toURI()))
              .build();
          final SequenceWriter writer = mapper.writer().withRootValueSeparator("\n").writeValues(outputFile)
      ) {
        Group group;
        while ((group = reader.read()) != null) {
          writer.write(converter.convertGroup(group));
        }
      }
    }
  }
}
