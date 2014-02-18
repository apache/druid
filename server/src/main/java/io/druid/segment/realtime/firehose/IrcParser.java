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

import com.ircclouds.irc.api.domain.messages.ChannelPrivMsg;
import com.metamx.common.Pair;
import com.metamx.common.exception.FormattedException;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import org.joda.time.DateTime;

/**
 */
public class IrcParser implements InputRowParser<Pair<DateTime, ChannelPrivMsg>>
{
  private final IrcDecoder decoder;

  public IrcParser(IrcDecoder decoder)
  {
    this.decoder = decoder;
  }

  @Override
  public InputRow parse(Pair<DateTime, ChannelPrivMsg> msg) throws FormattedException
  {
    return decoder.decodeMessage(msg.lhs, msg.rhs.getChannelName(), msg.rhs.getText());
  }

  @Override
  public ParseSpec getParseSpec()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    throw new UnsupportedOperationException();
  }
}