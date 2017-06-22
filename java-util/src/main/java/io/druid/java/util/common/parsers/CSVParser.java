/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common.parsers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.collect.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CSVParser implements Parser<String, Object>
{
  private static final Function<String, Object> getValueFunction(
      final String listDelimiter,
      final Splitter listSplitter
  )
  {
    return new Function<String, Object>()
    {
      @Override
      public Object apply(String input)
      {
        if (input.contains(listDelimiter)) {
          return Lists.newArrayList(
              Iterables.transform(
                  listSplitter.split(input),
                  ParserUtils.nullEmptyStringFunction
              )
          );
        } else {
          return ParserUtils.nullEmptyStringFunction.apply(input);
        }
      }
    };
  }

  private final String listDelimiter;
  private final Splitter listSplitter;
  private final Function<String, Object> valueFunction;
  private final boolean hasHeaderRow;
  private final int maxSkipHeaderRows;

  private final au.com.bytecode.opencsv.CSVParser parser = new au.com.bytecode.opencsv.CSVParser();

  private ArrayList<String> fieldNames = null;
  private boolean hasParsedHeader = false;
  private int skippedHeaderRows;
  private boolean supportSkipHeaderRows;

  public CSVParser(
      final Optional<String> listDelimiter,
      final boolean hasHeaderRow,
      final int maxSkipHeaderRows
  )
  {
    this.listDelimiter = listDelimiter.isPresent() ? listDelimiter.get() : Parsers.DEFAULT_LIST_DELIMITER;
    this.listSplitter = Splitter.on(this.listDelimiter);
    this.valueFunction = getValueFunction(this.listDelimiter, this.listSplitter);

    this.hasHeaderRow = hasHeaderRow;
    this.maxSkipHeaderRows = maxSkipHeaderRows;
  }

  public CSVParser(
      final Optional<String> listDelimiter,
      final Iterable<String> fieldNames,
      final boolean hasHeaderRow,
      final int maxSkipHeaderRows
  )
  {
    this(listDelimiter, hasHeaderRow, maxSkipHeaderRows);

    setFieldNames(fieldNames);
  }

  @VisibleForTesting
  CSVParser(final Optional<String> listDelimiter, final String header)
  {
    this(listDelimiter, false, 0);

    setFieldNames(header);
  }

  public String getListDelimiter()
  {
    return listDelimiter;
  }

  @Override
  public void startFileFromBeginning()
  {
    if (hasHeaderRow) {
      fieldNames = null;
    }
    hasParsedHeader = false;
    skippedHeaderRows = 0;
    supportSkipHeaderRows = true;
  }

  @Override
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @Override
  public void setFieldNames(final Iterable<String> fieldNames)
  {
    if (fieldNames != null) {
      ParserUtils.validateFields(fieldNames);
      this.fieldNames = Lists.newArrayList(fieldNames);
    }
  }

  public void setFieldNames(final String header)
  {
    try {
      setFieldNames(Arrays.asList(parser.parseLine(header)));
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse header [%s]", header);
    }
  }

  @Override
  public Map<String, Object> parse(final String input)
  {
    if (!supportSkipHeaderRows && (hasHeaderRow || maxSkipHeaderRows > 0)) {
      throw new UnsupportedOperationException("hasHeaderRow or maxSkipHeaderRows is not supported. "
                                              + "Please check the indexTask supports these options.");
    }

    try {
      String[] values = parser.parseLine(input);

      if (skippedHeaderRows < maxSkipHeaderRows) {
        skippedHeaderRows++;
        return null;
      }

      if (hasHeaderRow && !hasParsedHeader) {
        if (fieldNames == null) {
          setFieldNames(Arrays.asList(values));
        }
        hasParsedHeader = true;
        return null;
      }

      if (fieldNames == null) {
        setFieldNames(ParserUtils.generateFieldNames(values.length));
      }

      return Utils.zipMapPartial(fieldNames, Iterables.transform(Lists.newArrayList(values), valueFunction));
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse row [%s]", input);
    }
  }
}
