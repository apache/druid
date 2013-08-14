package com.metamx.druid.realtime.firehose;

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
import com.metamx.common.logger.Logger;
import com.metamx.druid.input.InputRow;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Example usage
 * <pre>
    Map<String, String> namespaces = Maps.newHashMap();
    namespaces.put("", "main");
    namespaces.put("Category", "category");
    namespaces.put("Media", "media");
    namespaces.put("MediaWiki", "mediawiki");
    namespaces.put("Template", "template");
    namespaces.put("$1 talk", "project talk");
    namespaces.put("Help talk", "help talk");
    namespaces.put("User", "user");
    namespaces.put("Template talk", "template talk");
    namespaces.put("MediaWiki talk", "mediawiki talk");
    namespaces.put("Talk", "talk");
    namespaces.put("Help", "help");
    namespaces.put("File talk", "file talk");
    namespaces.put("File", "file");
    namespaces.put("User talk", "user talk");
    namespaces.put("Special", "special");
    namespaces.put("Category talk", "category talk");

    IrcFirehoseFactory factory = new IrcFirehoseFactory(
        "wiki-druid-123",
        "irc.wikimedia.org",
        Lists.newArrayList(
            "#en.wikipedia",
            "#fr.wikipedia",
            "#de.wikipedia",
            "#ja.wikipedia"
        ),
        new WikipediaIrcDecoder(namespaces)
    );
   </pre>
 */
public class IrcFirehoseFactory implements FirehoseFactory
{
  private static final Logger log = new Logger(IrcFirehoseFactory.class);

  private final String nick;
  private final String host;
  private final List<String> channels;
  private final IrcDecoder decoder;

  @JsonCreator
  public IrcFirehoseFactory(
      @JsonProperty String nick,
      @JsonProperty String host,
      @JsonProperty List<String> channels,
      @JsonProperty IrcDecoder decoder
  )
  {
    this.nick = nick;
    this.host = host;
    this.channels = channels;
    this.decoder = decoder;
  }

  @Override
  public Firehose connect() throws IOException
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
            return Lists.newArrayList(nick + "_",
                                      nick + "__",
                                      nick + "___");
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
              nextRow = decoder.decodeMessage(nextMsg.lhs, nextMsg.rhs.getChannelName(), nextMsg.rhs.getText());
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
}

