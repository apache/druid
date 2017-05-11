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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.java.util.common.parsers.Parser;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Map;

/**
 */
public class StringInputRowParser implements ByteBufferInputRowParser
{
  private static final Charset DEFAULT_CHARSET = Charsets.UTF_8;

  private final ParseSpec parseSpec;
  private final MapInputRowParser mapParser;
  private final Parser<String, Object> parser;
  private final Charset charset;

  private CharBuffer chars = null;

  @JsonCreator
  public StringInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("encoding") String encoding
  )
  {
    this.parseSpec = parseSpec;
    this.mapParser = new MapInputRowParser(parseSpec);
    this.parser = parseSpec.makeParser();

    if (encoding != null) {
      this.charset = Charset.forName(encoding);
    } else {
      this.charset = DEFAULT_CHARSET;
    }
  }

  @Deprecated
  public StringInputRowParser(ParseSpec parseSpec)
  {
    this(parseSpec, null);
  }

  @Override
  public InputRow parse(ByteBuffer input)
  {
    return parseMap(buildStringKeyMap(input));
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @JsonProperty
  public String getEncoding()
  {
    return charset.name();
  }

  @Override
  public StringInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new StringInputRowParser(parseSpec, getEncoding());
  }

  private Map<String, Object> buildStringKeyMap(ByteBuffer input)
  {
    int payloadSize = input.remaining();

    if (chars == null || chars.remaining() < payloadSize) {
      chars = CharBuffer.allocate(payloadSize);
    }

    final CoderResult coderResult = charset.newDecoder()
                                           .onMalformedInput(CodingErrorAction.REPLACE)
                                           .onUnmappableCharacter(CodingErrorAction.REPLACE)
                                           .decode(input, chars, true);

    Map<String, Object> theMap;
    if (coderResult.isUnderflow()) {
      chars.flip();
      try {
        theMap = parseString(chars.toString());
      }
      finally {
        chars.clear();
      }
    } else {
      throw new ParseException("Failed with CoderResult[%s]", coderResult);
    }
    return theMap;
  }

  public void startFileFromBeginning()
  {
    parser.startFileFromBeginning();
  }

  @Nullable
  public InputRow parse(@Nullable String input)
  {
    return parseMap(parseString(input));
  }

  @Nullable
  private Map<String, Object> parseString(@Nullable String inputString)
  {
    return parser.parse(inputString);
  }

  @Nullable
  private InputRow parseMap(@Nullable Map<String, Object> theMap)
  {
    // If a header is present in the data (and with proper configurations), a null is returned
    if (theMap == null) {
      return null;
    }
    return mapParser.parse(theMap);
  }
}
