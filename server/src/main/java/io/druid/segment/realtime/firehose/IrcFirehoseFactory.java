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

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.ircclouds.irc.api.Callback;
import com.ircclouds.irc.api.IRCApi;
import com.ircclouds.irc.api.IRCApiImpl;
import com.ircclouds.irc.api.IServerParameters;
import com.ircclouds.irc.api.domain.IRCServer;
import com.ircclouds.irc.api.domain.messages.ChannelPrivMsg;
import com.ircclouds.irc.api.listeners.VariousMessageListenerAdapter;
import com.ircclouds.irc.api.state.IIRCState;

import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;

import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * <p><b>Example code:</b></p>
 * <pre>{@code
 * <p/>
 * IrcFirehoseFactory factory = new IrcFirehoseFactory(
 *     "wiki123",
 *     "irc.wikimedia.org",
 *     Lists.newArrayList(
 *         "#en.wikipedia",
 *         "#fr.wikipedia",
 *         "#de.wikipedia",
 *         "#ja.wikipedia"
 *     )
 * );
 * }</pre>
 */
public class IrcFirehoseFactory implements FirehoseFactory<IrcInputRowParser>
{
  private static final Logger log = new Logger(IrcFirehoseFactory.class);

  private final String nick;
  private final String host;
  private final List<String> channels;
  private volatile boolean closed = false;

  @JsonCreator
  public IrcFirehoseFactory(
      @JsonProperty("nick") String nick,
      @JsonProperty("host") String host,
      @JsonProperty("channels") List<String> channels
  )
  {
    this.nick = nick;
    this.host = host;
    this.channels = channels;
  }

  @JsonProperty
  public String getNick()
  {
    return nick;
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  public List<String> getChannels()
  {
    return channels;
  }

  @Override
  public Firehose connect(final IrcInputRowParser firehoseParser) throws IOException
  {
    final IRCApi irc = new IRCApiImpl(false);
    final LinkedBlockingQueue<Pair<DateTime, ChannelPrivMsg>> queue = new LinkedBlockingQueue<Pair<DateTime, ChannelPrivMsg>>();

    irc.addListener(
        new VariousMessageListenerAdapter()
        {
          @Override
          public void onChannelMessage(ChannelPrivMsg aMsg)
          {
            try {
              queue.put(Pair.of(DateTime.now(), aMsg));
            }
            catch (InterruptedException e) {
              throw new RuntimeException("interrupted adding message to queue", e);
            }
          }
        }
    );

    log.info("connecting to irc server [%s]", host);
    irc.connect(
        new IServerParameters()
        {
          @Override
          public String getNickname()
          {
            return nick;
          }

          @Override
          public List<String> getAlternativeNicknames()
          {
            return Lists.newArrayList(nick + UUID.randomUUID(), nick + UUID.randomUUID(), nick + UUID.randomUUID());
          }

          @Override
          public String getIdent()
          {
            return "druid";
          }

          @Override
          public String getRealname()
          {
            return nick;
          }

          @Override
          public IRCServer getServer()
          {
            return new IRCServer(host, false);
          }
        },
        new Callback<IIRCState>()
        {
          @Override
          public void onSuccess(IIRCState aObject)
          {
            log.info("irc connection to server [%s] established", host);
            for (String chan : channels) {
              log.info("Joining channel %s", chan);
              irc.joinChannel(chan);
            }
          }

          @Override
          public void onFailure(Exception e)
          {
            log.error(e, "Unable to connect to irc server [%s]", host);
            throw new RuntimeException("Unable to connect to server", e);
          }
        }
    );

    closed = false;

    return new Firehose()
    {
      InputRow nextRow = null;

      @Override
      public boolean hasMore()
      {
        try {
          while (true) {
            Pair<DateTime, ChannelPrivMsg> nextMsg = queue.poll(100, TimeUnit.MILLISECONDS);
            if (closed) {
              return false;
            }
            if (nextMsg == null) {
              continue;
            }
            try {
              nextRow = firehoseParser.parse(nextMsg);
              if (nextRow != null) {
                return true;
              }
            }
            catch (IllegalArgumentException iae) {
              log.debug("ignoring invalid message in channel [%s]", nextMsg.rhs.getChannelName());
            }
          }
        }
        catch (InterruptedException e) {
          Thread.interrupted();
          throw new RuntimeException("interrupted retrieving elements from queue", e);
        }
      }

      @Override
      public InputRow nextRow()
      {
        return nextRow;
      }

      @Override
      public Runnable commit()
      {
        return new Runnable()
        {
          @Override
          public void run()
          {
            // nothing to see here
          }
        };
      }

      @Override
      public void close() throws IOException
      {
        try {
          log.info("disconnecting from irc server [%s]", host);
          irc.disconnect("");
        }
        finally {
          closed = true;
        }
      }
    };
  }
}

