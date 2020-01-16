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

package org.apache.druid.data.input.impl;

import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DelimitedValueReader.DelimitedValueParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CSVParser implements DelimitedValueParser
{
  private static final char SEPERATOR = ',';
  private final RFC4180Parser delegate;

  public static RFC4180Parser createOpenCsvParser()
  {
    return NullHandling.replaceWithDefault()
           ? new RFC4180ParserBuilder().withSeparator(SEPERATOR).build()
           : new RFC4180ParserBuilder().withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS)
                                       .withSeparator(SEPERATOR)
                                       .build();
  }

  CSVParser()
  {
    delegate = createOpenCsvParser();
  }

  @Override
  public List<String> parseLine(String line) throws IOException
  {
    return Arrays.asList(delegate.parseLine(line));
  }
}
