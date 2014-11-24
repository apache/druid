/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.ircclouds.irc.api.domain.messages.ChannelPrivMsg;
import com.metamx.common.Pair;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import org.joda.time.DateTime;

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
  public InputRow parse(Pair<DateTime, ChannelPrivMsg> msg)
  {
    return decoder.decodeMessage(msg.lhs, msg.rhs.getChannelName(), msg.rhs.getText());
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