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
import com.google.common.collect.Lists;
import com.ircclouds.irc.api.Callback;
import com.ircclouds.irc.api.IRCApi;
import com.ircclouds.irc.api.IRCApiImpl;
import com.ircclouds.irc.api.IServerParameters;
import com.ircclouds.irc.api.domain.IRCServer;
import com.ircclouds.irc.api.domain.messages.ChannelPrivMsg;
import com.ircclouds.irc.api.listeners.VariousMessageListenerAdapter;
import com.ircclouds.irc.api.state.IIRCState;
import com.metamx.common.Pair;
import com.metamx.common.exception.FormattedException;
import com.metamx.common.logger.Logger;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import org.joda.time.DateTime;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * <p><b>Example Usage</b></p>
 *
 * <p>Decoder definition: <code>wikipedia-decoder.json</code></p>
 * <pre>{@code
 *
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
 *
 * <p><b>Example code:</b></p>
 * <pre>{@code
 * IrcDecoder wikipediaDecoder = new ObjectMapper().readValue(
 *   new File("wikipedia-decoder.json"),
 *   IrcDecoder.class
 * );
 *
 * IrcFirehoseFactory factory = new IrcFirehoseFactory(
 *     "wiki123",
 *     "irc.wikimedia.org",
 *     Lists.newArrayList(
 *         "#en.wikipedia",
 *         "#fr.wikipedia",
 *         "#de.wikipedia",
 *         "#ja.wikipedia"
 *     ),
 *     wikipediaDecoder
 * );
 * }</pre>
 */
public class IrcFirehoseFactory implements FirehoseFactory<IrcParser>
{
  private static final Logger log = new Logger(IrcFirehoseFactory.class);

  private final String nick;
  private final String host;
  private final List<String> channels;
  private final IrcParser parser;

  @JsonCreator
  public IrcFirehoseFactory(
      @JsonProperty("name") String nick,
      @JsonProperty("host") String host,
      @JsonProperty("channels") List<String> channels,
      @JsonProperty("decoder") IrcDecoder decoder
  )
  {
    this.nick = nick;
    this.host = host;
    this.channels = channels;
    this.parser = new IrcParser(decoder);
  }

  @Override
  public Firehose connect(final IrcParser firehoseParser) throws IOException
  {
    final IRCApi irc = new IRCApiImpl(false);
    final LinkedBlockingQueue<Pair<DateTime, ChannelPrivMsg>> queue = new LinkedBlockingQueue<Pair<DateTime, ChannelPrivMsg>>();

    irc.addListener(new VariousMessageListenerAdapter() {
      @Override
      public void onChannelMessage(ChannelPrivMsg aMsg)
      {
        try {
          queue.put(Pair.of(DateTime.now(), aMsg));
        } catch(InterruptedException e) {
          throw new RuntimeException("interrupted adding message to queue", e);
        }
      }
    });

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
            for(String chan : channels) {
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
        });


    return new Firehose()
    {
      InputRow nextRow = null;

      @Override
      public boolean hasMore()
      {
        try {
          while(true) {
            Pair<DateTime, ChannelPrivMsg> nextMsg = queue.take();
            try {
              nextRow = firehoseParser.parse(nextMsg);
              if(nextRow != null) return true;
            }
            catch (IllegalArgumentException iae) {
              log.debug("ignoring invalid message in channel [%s]", nextMsg.rhs.getChannelName());
            }
          }
        }
        catch(InterruptedException e) {
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
        log.info("disconnecting from irc server [%s]", host);
        irc.disconnect("");
      }
    };
  }

  @Override
  public IrcParser getParser()
  {
    return parser;
  }
}

