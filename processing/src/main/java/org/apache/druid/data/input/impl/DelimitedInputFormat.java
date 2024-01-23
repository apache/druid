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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * InputFormat for customized Delimiter Separate Value format of input data (default is TSV).
 */
public class DelimitedInputFormat extends FlatTextInputFormat
{
  public static final String TYPE_KEY = "tsv";

  private static final String DEFAULT_DELIMITER = "\t";

  @JsonCreator
  public DelimitedInputFormat(
      @JsonProperty("columns") @Nullable List<String> columns,
      @JsonProperty("listDelimiter") @Nullable String listDelimiter,
      @JsonProperty("delimiter") @Nullable String delimiter,
      @Deprecated @JsonProperty("hasHeaderRow") @Nullable Boolean hasHeaderRow,
      @JsonProperty("findColumnsFromHeader") @Nullable Boolean findColumnsFromHeader,
      @JsonProperty("skipHeaderRows") int skipHeaderRows
  )
  {
    super(
        columns,
        listDelimiter,
        delimiter == null ? DEFAULT_DELIMITER : delimiter,
        hasHeaderRow,
        findColumnsFromHeader,
        skipHeaderRows
    );
  }

  @Override
  public boolean isSplittable()
  {
    return true;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new DelimitedValueReader(
        inputRowSchema,
        source,
        getListDelimiter(),
        getColumns(),
        isFindColumnsFromHeader(),
        getSkipHeaderRows(),
        makeDelimitedValueParser(
            getDelimiter(),
            useListBasedInputRows() ? getColumns().size() : DelimitedBytes.UNKNOWN_FIELD_COUNT
        ),
        useListBasedInputRows()
    );
  }

  /**
   * Create a parser for a particular delimiter and expected number of fields.
   */
  private static DelimitedValueReader.DelimitedValueParser makeDelimitedValueParser(
      final String delimiter,
      final int numFields
  )
  {
    final byte[] utf8Delimiter = StringUtils.toUtf8(delimiter);
    if (utf8Delimiter.length == 1) {
      // Single-byte delimiter: split bytes directly, prior to UTF-8 conversion
      final byte delimiterByte = utf8Delimiter[0];
      return bytes -> DelimitedBytes.split(bytes, delimiterByte, numFields);
    } else {
      final Splitter splitter = Splitter.on(delimiter);
      return bytes -> splitToList(splitter, StringUtils.fromUtf8(bytes));
    }
  }

  /**
   * Copied from Guava's {@link Splitter#splitToList(CharSequence)}.
   * This is to avoid the error of the missing method signature when using an old Guava library.
   * For example, it may happen when running Druid Hadoop indexing jobs, since we may inherit the version provided by
   * the Hadoop cluster. See https://github.com/apache/druid/issues/6801.
   */
  public static List<String> splitToList(Splitter splitter, String input)
  {
    Preconditions.checkNotNull(input);

    Iterator<String> iterator = splitter.split(input).iterator();
    List<String> result = new ArrayList<>();

    while (iterator.hasNext()) {
      String splitValue = iterator.next();
      if (!NullHandling.replaceWithDefault() && splitValue.isEmpty()) {
        result.add(null);
      } else {
        result.add(splitValue);
      }
    }

    return Collections.unmodifiableList(result);
  }

  @Override
  public String toString()
  {
    return "DelimitedInputFormat{" + fieldsToString() + "}";
  }
}
