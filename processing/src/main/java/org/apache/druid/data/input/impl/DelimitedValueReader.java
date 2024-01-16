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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.TextReader;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.ParserUtils;
import org.apache.druid.java.util.common.parsers.Parsers;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * DelimitedValueReader is the reader for Delimitor Separate Value format input data(CSV/TSV).
 */
public class DelimitedValueReader extends TextReader.Bytes
{
  private final boolean findColumnsFromHeader;
  private final int skipHeaderRows;
  private final Function<String, Object> multiValueFunction;
  private final DelimitedValueParser parser;

  /**
   * Signature of the delimited files. Written by {@link #setSignature(List)}.
   */
  @Nullable
  private RowSignature inputRowSignature;

  /**
   * Dimensions list used for generating {@link InputRow}. Derived from {@link #getInputRowSchema()}, but has
   * dimensions locked-in based on the {@link #inputRowSignature}, so they do not need to be recalculated on each row.
   * Written by {@link #setSignature(List)}.
   */
  @Nullable
  private List<String> inputRowDimensions;
  private final boolean useListBasedInputRows;

  interface DelimitedValueParser
  {
    List<String> parseLine(byte[] line) throws IOException;
  }

  DelimitedValueReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      @Nullable String listDelimiter,
      @Nullable List<String> columns,
      boolean findColumnsFromHeader,
      int skipHeaderRows,
      DelimitedValueParser parser,
      boolean useListBasedInputRows
  )
  {
    super(inputRowSchema, source);
    this.findColumnsFromHeader = findColumnsFromHeader;
    this.skipHeaderRows = skipHeaderRows;
    final String finalListDelimeter = listDelimiter == null ? Parsers.DEFAULT_LIST_DELIMITER : listDelimiter;
    this.multiValueFunction = ParserUtils.getMultiValueFunction(finalListDelimeter, Splitter.on(finalListDelimeter));

    if (!findColumnsFromHeader && columns != null) {
      // If findColumnsFromHeader, inputRowSignature will be set later.
      setSignature(columns);
    }

    this.parser = parser;
    this.useListBasedInputRows = useListBasedInputRows;
  }

  @Override
  public List<InputRow> parseInputRows(byte[] line) throws IOException, ParseException
  {
    if (useListBasedInputRows) {
      final List<Object> parsed = readLineAsList(line);
      return Collections.singletonList(
          ListBasedInputRow.parse(
              inputRowSignature,
              getInputRowSchema().getTimestampSpec(),
              inputRowDimensions,
              parsed
          )
      );
    } else {
      final Map<String, Object> zipped = readLineAsMap(line);
      return Collections.singletonList(
          MapInputRowParser.parse(
              getInputRowSchema().getTimestampSpec(),
              inputRowDimensions,
              zipped
          )
      );
    }
  }

  @Override
  public List<Map<String, Object>> toMap(byte[] intermediateRow) throws IOException
  {
    return Collections.singletonList(readLineAsMap(intermediateRow));
  }

  private List<Object> readLineAsList(byte[] line) throws IOException
  {
    final List<String> parsed = parser.parseLine(line);
    return new ArrayList<>(Lists.transform(parsed, multiValueFunction));
  }

  private Map<String, Object> readLineAsMap(byte[] line) throws IOException
  {
    final List<String> parsed = parser.parseLine(line);
    return Utils.zipMapPartial(
        Preconditions.checkNotNull(inputRowSignature, "inputRowSignature").getColumnNames(),
        Iterables.transform(parsed, multiValueFunction)
    );
  }

  @Override
  public int getNumHeaderLinesToSkip()
  {
    return skipHeaderRows;
  }

  @Override
  public boolean needsToProcessHeaderLine()
  {
    return findColumnsFromHeader;
  }

  @Override
  public void processHeaderLine(byte[] line) throws IOException
  {
    if (!findColumnsFromHeader) {
      throw new ISE("Don't call this if findColumnsFromHeader = false");
    }
    setSignature(parser.parseLine(line));
  }

  /**
   * Set {@link #inputRowDimensions} and {@link #inputRowSignature} based on a set of header columns. Must be called
   * prior to {@link #parseInputRows(byte[])}.
   *
   * @param columns header columns
   */
  private void setSignature(final List<String> columns)
  {
    inputRowSignature = findOrCreateInputRowSignature(columns);
    if (inputRowSignature.size() == 0) {
      throw new ISE("Empty columns");
    }

    inputRowDimensions = MapInputRowParser.findDimensions(
        getInputRowSchema().getTimestampSpec(),
        getInputRowSchema().getDimensionsSpec(),
        ImmutableSet.copyOf(inputRowSignature.getColumnNames())
    );
  }
}
