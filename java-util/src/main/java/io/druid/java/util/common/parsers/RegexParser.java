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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.collect.Utils;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 */
public class RegexParser implements Parser<String, Object>
{
  private final String pattern;
  private final Splitter listSplitter;
  private final Function<String, Object> valueFunction;
  private final Pattern compiled;

  private List<String> fieldNames = null;

  public RegexParser(
      final String pattern,
      final Optional<String> listDelimiter
  )
  {
    this.pattern = pattern;
    this.listSplitter = Splitter.onPattern(listDelimiter.isPresent()
                                           ? listDelimiter.get()
                                           : Parsers.DEFAULT_LIST_DELIMITER);
    this.valueFunction = new Function<String, Object>()
    {
      @Override
      public Object apply(String input)
      {
        final List retVal = StreamSupport.stream(listSplitter.split(input).spliterator(), false)
                                         .map(Strings::emptyToNull)
                                         .collect(Collectors.toList());
        if (retVal.size() == 1) {
          return retVal.get(0);
        } else {
          return retVal;
        }
      }
    };
    this.compiled = Pattern.compile(pattern);
  }

  public RegexParser(
      final String pattern,
      final Optional<String> listDelimiter,
      final Iterable<String> fieldNames
  )
  {
    this(pattern, listDelimiter);

    setFieldNames(fieldNames);
  }

  @Override
  public Map<String, Object> parse(String input)
  {
    try {
      final Matcher matcher = compiled.matcher(input);

      if (!matcher.matches()) {
        throw new ParseException("Incorrect Regex: %s . No match found.", pattern);
      }

      List<String> values = Lists.newArrayList();
      for (int i = 1; i <= matcher.groupCount(); i++) {
        values.add(matcher.group(i));
      }

      if (fieldNames == null) {
        setFieldNames(ParserUtils.generateFieldNames(values.size()));
      }

      return Utils.zipMapPartial(fieldNames, Iterables.transform(values, valueFunction));
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse row [%s]", input);
    }
  }

  @Override
  public void setFieldNames(Iterable<String> fieldNames)
  {
    ParserUtils.validateFields(fieldNames);
    this.fieldNames = Lists.newArrayList(fieldNames);
  }

  @Override
  public List<String> getFieldNames()
  {
    return fieldNames;
  }
}
