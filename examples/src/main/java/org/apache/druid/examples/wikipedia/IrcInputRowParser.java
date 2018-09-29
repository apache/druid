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

package org.apache.druid.examples.wikipedia;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.ircclouds.irc.api.domain.messages.ChannelPrivMsg;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.Pair;
import org.joda.time.DateTime;

import java.util.List;

/**
 * <p><b>Example Usage</b></p>
 * <p/>
 * <p>Decoder definition: <code>wikipedia-decoder.json</code></p>
 * <pre>{@code
 * <p/>
 * {
 *   "type": "wikipedia",
 *   "namespaces": {
 *     "#en.wikipedia": {
 *       "": "main",
 *       "Category": "category",
 *       "Template talk": "template talk",
 *       "Help talk": "help talk",
 *       "Media": "media",
 *       "MediaWiki talk": "mediawiki talk",
 *       "File talk": "file talk",
 *       "MediaWiki": "mediawiki",
 *       "User": "user",
 *       "File": "file",
 *       "User talk": "user talk",
 *       "Template": "template",
 *       "Help": "help",
 *       "Special": "special",
 *       "Talk": "talk",
 *       "Category talk": "category talk"
 *     }
 *   },
 *   "geoIpDatabase": "path/to/GeoLite2-City.mmdb"
 * }
 * }</pre>
 */
@JsonTypeName("irc")
public class IrcInputRowParser implements InputRowParser<Pair<DateTime, ChannelPrivMsg>>
{
  private final ParseSpec parseSpec;
  private final IrcDecoder decoder;

  @JsonCreator
  public IrcInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("decoder") IrcDecoder decoder
  )
  {
    this.parseSpec = parseSpec;
    this.decoder = decoder;
  }

  @JsonProperty
  public IrcDecoder getDecoder()
  {
    return decoder;
  }

  @Override
  public List<InputRow> parseBatch(Pair<DateTime, ChannelPrivMsg> msg)
  {
    return ImmutableList.of(decoder.decodeMessage(msg.lhs, msg.rhs.getChannelName(), msg.rhs.getText()));
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new IrcInputRowParser(parseSpec, decoder);
  }
}
