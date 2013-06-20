//package druid.examples.twitter;
//
///*
// * Copyright 2007 Yusuke Yamamoto
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//
//
//import org.mortbay.jetty.servlet.Dispatcher;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//
///**
// * @author Yusuke Yamamoto - yusuke at mac.com
// * @since Twitter4J 2.1.8
// */
//abstract class WebStatusStreamBase implements WebStatusStream
//{
//  protected static final twitter4j.internal.logging.Logger logger = twitter4j.internal
//      .logging
//      .Logger
//      .getLogger(WebStatusStreamImpl.class);
//
//  private boolean streamAlive = true;
//  private BufferedReader br;
//  private InputStream is;
//  private HttpResponse response;
//  protected final Dispatcher dispatcher;
//  protected final Configuration CONF;
//  protected z_InternalFactory factory;
//
//    /*package*/
//
//  StatusStreamBase(Dispatcher dispatcher, InputStream stream, twitter4j.conf.Configuration conf) throws IOException
//  {
//    this.is = stream;
//    this.br = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
//    this.dispatcher = dispatcher;
//    this.CONF = conf;
//    this.factory = new z_T4JInternalJSONImplFactory(conf);
//  }
//    /*package*/
//
//  StatusStreamBase(Dispatcher dispatcher, HttpResponse response, twitter4j.conf.Configuration conf) throws IOException {
//    this(dispatcher, response.asStream(), conf);
//    this.response = response;
//  }
//
//  protected String parseLine(String line) {
//    return line;
//  }
//
//  abstract class StreamEvent implements Runnable {
//    String line;
//
//    StreamEvent(String line) {
//      this.line = line;
//    }
//  }
//
//  protected void handleNextElement(final StreamListener[] listeners,
//                                   final RawStreamListener[] rawStreamListeners) throws TwitterException
//  {
//    if (!streamAlive) {
//      throw new IllegalStateException("Stream already closed.");
//    }
//    try {
//      String line = br.readLine();
//      if (null == line) {
//        //invalidate this status stream
//        throw new IOException("the end of the stream has been reached");
//      }
//      dispatcher.invokeLater(new StreamEvent(line) {
//        public void run() {
//          try {
//            if (rawStreamListeners.length > 0) {
//              onMessage(line, rawStreamListeners);
//            }
//            // SiteStreamsImpl will parse "forUser" attribute
//            line = parseLine(line);
//            if (line != null && line.length() > 0) {
//              // parsing JSON is an expensive process and can be avoided when all listeners are instanceof RawStreamListener
//              if (listeners.length > 0) {
//                if (CONF.isJSONStoreEnabled()) {
//                  DataObjectFactoryUtil.clearThreadLocalMap();
//                }
//                twitter4j.internal.org.json.JSONObject json = new twitter4j.internal.org.json.JSONObject(line);
//                JSONObjectType.Type event = JSONObjectType.determine(json);
//                if (logger.isDebugEnabled()) {
//                  logger.debug("Received:", CONF.isPrettyDebugEnabled() ? json.toString(1) : json.toString());
//                }
//                switch (event) {
//                  case STATUS:
//                    onStatus(json, listeners);
//                    break;
//                  default:
//                    logger.warn("Received unknown event:", CONF.isPrettyDebugEnabled() ? json.toString(1) : json.toString());
//                }
//              }
//            }
//          } catch (Exception ex) {
//            onException(ex, listeners);
//          }
//        }
//      });
//
//    } catch (IOException ioe) {
//      try {
//        is.close();
//      } catch (IOException ignore) {
//      }
//      boolean isUnexpectedException = streamAlive;
//      streamAlive = false;
//      if (isUnexpectedException) {
//        throw new RuntimeException("Stream closed.", ioe);
//      }
//    }
//  }
//
//
//
//  protected void onStatus(twitter4j.internal.org.json.JSONObject json, StreamListener[] listeners) throws TwitterException {
//    logger.warn("Unhandled event: onStatus");
//  }
//
//
//  protected void onException(Exception e, StreamListener[] listeners) {
//    logger.warn("Unhandled event: ", e.getMessage());
//  }
//
//  public void close() throws IOException {
//    streamAlive = false;
//    is.close();
//    br.close();
//    if (response != null) {
//      response.disconnect();
//    }
//  }
//
//  protected Status asStatus(twitter4j.internal.org.json.JSONObject json) throws TwitterException {
//    Status status = factory.createStatus(json);
//    if (CONF.isJSONStoreEnabled()) {
//      DataObjectFactoryUtil.registerJSONObject(status, json);
//    }
//    return status;
//  }
//
//
//  public abstract void next(StatusListener listener) throws TwitterException;
//
//  public abstract void next(StreamListener[] listeners, RawStreamListener[] rawStreamListeners) throws TwitterException;
//
//  public void onException(Exception e, StreamListener[] listeners, RawStreamListener[] rawStreamListeners) {
//    for (StreamListener listener : listeners) {
//      listener.onException(e);
//    }
//    for (RawStreamListener listener : rawStreamListeners) {
//      listener.onException(e);
//    }
//  }
//}
