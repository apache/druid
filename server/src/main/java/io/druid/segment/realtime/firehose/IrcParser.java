/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 */
@JsonTypeName("protoBuf")
public class IrcParser implements InputRowParser<Pair<DateTime, ChannelPrivMsg>>
{
  private final IrcDecoder decoder;

  @JsonCreator
  public IrcParser(@JsonProperty("decoder") IrcDecoder decoder)
  {
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

  @Override
  public ParseSpec getParseSpec()
  {
    return null;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    throw new UnsupportedOperationException();
  }
}
