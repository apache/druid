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
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.FileEntity;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class BaseParquetReaderTest
{
  ObjectWriter DEFAULT_JSON_WRITER = new ObjectMapper().writerWithDefaultPrettyPrinter();

  InputEntityReader createReader(String parquetFile, InputRowSchema schema, JSONPathSpec flattenSpec)
  {
    return createReader(parquetFile, schema, flattenSpec, false);
  }

  InputEntityReader createReader(
      String parquetFile,
      InputRowSchema schema,
      JSONPathSpec flattenSpec,
      boolean binaryAsString
  )
  {
    FileEntity entity = new FileEntity(new File(parquetFile));
    ParquetInputFormat parquet = new ParquetInputFormat(flattenSpec, binaryAsString, new Configuration());
    return parquet.createReader(schema, entity, null);
  }

  List<InputRow> readAllRows(InputEntityReader reader) throws IOException
  {
    List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }

  List<InputRowListPlusRawValues> sampleAllRows(InputEntityReader reader) throws IOException
  {
    List<InputRowListPlusRawValues> rows = new ArrayList<>();
    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }
}
