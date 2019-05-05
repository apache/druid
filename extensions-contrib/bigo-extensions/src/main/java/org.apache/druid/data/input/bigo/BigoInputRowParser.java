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

package org.apache.druid.data.input.bigo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.Parser;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

//import jdk.nashorn.internal.ir.ObjectNode;

public class BigoInputRowParser implements InputRowParser<Object>
{
  private static final Logger log = new Logger(BigoInputRowParser.class);
  private static final Charset DEFAULT_CHARSET = Charsets.UTF_8;

  private final ParseSpec parseSpec;
  private final MapInputRowParser mapParser;
  private final Charset charset;
  private final String flattenField;
  private final boolean flumeEventOrNot;
  private final long badJsonTolThreshold;


  private Parser<String, Object> parser;
  private CharBuffer chars;
  private long exJsonCount = 0;

  @JsonCreator
  public BigoInputRowParser(
          @JsonProperty("parseSpec") ParseSpec parseSpec,
          @JsonProperty("encoding") String encoding,
          @JsonProperty("flattenField") String flattenField,
          @JsonProperty("flumeEventOrNot") boolean flumeEventOrNot,
          @JsonProperty("badJsonTolThreshold") long badJsonTolThreshold
  )
  {
    this.parseSpec = Preconditions.checkNotNull(parseSpec, "parseSpec");
    this.mapParser = new MapInputRowParser(parseSpec);
    this.flattenField = flattenField;
    this.flumeEventOrNot = flumeEventOrNot;
    this.badJsonTolThreshold = badJsonTolThreshold;

    if (encoding != null) {
      this.charset = Charset.forName(encoding);
    } else {
      this.charset = DEFAULT_CHARSET;
    }
  }


  @Override
  public List<InputRow> parseBatch(Object input)
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
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new BigoInputRowParser(parseSpec, getEncoding(), flattenField, flumeEventOrNot, badJsonTolThreshold);
  }

  private List<Map<String, Object>> buildStringKeyMap(Object input)
  {
    String inputStr = null;
    ByteBuffer inputBuffer = null;
    if (input instanceof Text) {
      inputStr = ((Text) input).toString();
    } else if (input instanceof BytesWritable) {
      BytesWritable valueBytes = (BytesWritable) input;
      inputBuffer = ByteBuffer.wrap(valueBytes.getBytes(), 0, valueBytes.getLength());
    } else if (input instanceof ByteBuffer) {
      inputBuffer = (ByteBuffer) input;
    } else {
      throw new IAE("can't convert type [%s] to InputRow", input.getClass().getName());
    }

    List<Map<String, Object>> theMap;
    if (inputStr != null && inputStr.length() != 0) {
      theMap = parseString(inputStr);
    } else {
      int payloadSize = inputBuffer.remaining();

      if (chars == null || chars.remaining() < payloadSize) {
        chars = CharBuffer.allocate(payloadSize);
      }

      final CoderResult coderResult = charset.newDecoder()
              .onMalformedInput(CodingErrorAction.REPLACE)
              .onUnmappableCharacter(CodingErrorAction.REPLACE)
              .decode(inputBuffer, chars, true);


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
    }

    return theMap;
  }

  public void initializeParser()
  {
    if (parser == null) {
      // parser should be created when it is really used to avoid unnecessary initialization of the underlying
      // parseSpec.
      parser = parseSpec.makeParser();
    }
  }

  public void startFileFromBeginning()
  {
    initializeParser();
    parser.startFileFromBeginning();
  }


  @Nullable
  private List<Map<String, Object>> parseString(@Nullable String inputString)
  {
    initializeParser();
    if (flumeEventOrNot) {
      inputString = "{\"rip\"" + inputString.split("\"rip\"")[1];
    }

    boolean isBadJson = false;
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
    List<Map<String, Object>> returnValue = new ArrayList<>();
    try {
      JsonNode document = objectMapper.readValue(inputString, JsonNode.class);
      JsonNode flattenNode = document.findPath(flattenField);
      Iterator<JsonNode> elements = flattenNode.elements();
      while (elements.hasNext()) {
        JsonNode node = elements.next();
        JsonNode documentCopy = document.deepCopy();
        ((ObjectNode) documentCopy).remove(flattenField);
        ((ObjectNode) documentCopy).put(flattenField, node);

        Map<String, Object> oneReturn = parser.parseToMap(documentCopy.toString());
        returnValue.add(oneReturn);
      }
    }
    catch (Exception e) {
      log.error(e, "## Unable to parse row [%s]", inputString);
      exJsonCount++;
      isBadJson = true;
      if (exJsonCount > badJsonTolThreshold && badJsonTolThreshold != -1) {
        log.error(e, "@@ Probrom json count is max than:[%d]", exJsonCount);
        throw new ParseException(e, " $$ Unable to parse row [%s]！ Probrom json count is max than:[%s]", inputString, String.valueOf(exJsonCount));
      }
    }

    if (returnValue.size() == 0 && !isBadJson) {
      returnValue.add(parser.parseToMap(inputString));
    }

    return returnValue;
  }

  @Nullable
  private List<InputRow> parseMap(@Nullable List<Map<String, Object>> theMap)
  {
    // If a header is present in the data (and with proper configurations), a null is returned
    if (theMap == null) {
      return null;
    }
    List<InputRow> allList = new ArrayList<>();
    for (Map<String, Object> oneMap: theMap) {
      List<InputRow> oneList = mapParser.parseBatch(oneMap);
      allList.addAll(oneList);
    }
    return allList;
  }
}
