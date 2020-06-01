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

package org.apache.druid.metadata.input;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.prefetch.JsonIterator;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Reader exclusively for {@link SqlEntity}
 */
public class SqlReader extends IntermediateRowParsingReader<Map<String, Object>>
{
  private final InputRowSchema inputRowSchema;
  private final SqlEntity source;
  private final File temporaryDirectory;
  private final ObjectMapper objectMapper;


  SqlReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory,
      ObjectMapper objectMapper
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.source = (SqlEntity) source;
    this.temporaryDirectory = temporaryDirectory;
    this.objectMapper = objectMapper;
  }

  @Override
  protected CloseableIterator<Map<String, Object>> intermediateRowIterator() throws IOException
  {
    final Closer closer = Closer.create();
    //The results are fetched into local storage as this avoids having to keep a persistent database connection for a long time
    final InputEntity.CleanableFile resultFile = closer.register(source.fetch(temporaryDirectory, null));
    FileInputStream inputStream = new FileInputStream(resultFile.file());
    JsonIterator<Map<String, Object>> jsonIterator = new JsonIterator<>(new TypeReference<Map<String, Object>>()
    {
    }, inputStream, closer, objectMapper);
    return jsonIterator;
  }

  @Override
  protected List<InputRow> parseInputRows(Map<String, Object> intermediateRow) throws ParseException
  {
    return Collections.singletonList(
        MapInputRowParser.parse(
            inputRowSchema.getTimestampSpec(),
            inputRowSchema.getDimensionsSpec(),
            intermediateRow
        )
    );
  }

  @Override
  protected Map<String, Object> toMap(Map<String, Object> intermediateRow)
  {
    return intermediateRow;
  }
}
