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
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * A java representation of the <a href="https://dev.twitter.com/docs/streaming-api/methods">Streaming API: Methods</a><br>
// * Note that this class is NOT compatible with Google App Engine as GAE is not capable of handling requests longer than 30 seconds.
// *
// * @author Yusuke Yamamoto - yusuke at mac.com
// * @since Twitter4J 2.0.4
// */
//class WebStreamImpl extends TwitterBaseImpl implements WebStream
//{
//  private static final long serialVersionUID = 5529611191443189901L;
//  private final HttpClientWrapper http;
//  private static final Logger logger = Logger.getLogger(TwitterStreamImpl.class);
//
//  private List<ConnectionLifeCycleListener> lifeCycleListeners = new ArrayList<ConnectionLifeCycleListener>(0);
//  private TwitterStreamConsumer handler = null;
//
//  private String stallWarningsGetParam;
//  private HttpParameter stallWarningsParam;
//
//  /*package*/
//  TwitterStreamImpl(twitter4j.conf.Configuration conf, Authorization auth) {
//    super(conf, auth);
//    http = new HttpClientWrapper(new StreamingReadTimeoutConfiguration(conf));
//    stallWarningsGetParam = "stall_warnings=" + (conf.isStallWarningsEnabled() ? "true" : "false");
//    stallWarningsParam = new HttpParameter("stall_warnings", conf.isStallWarningsEnabled());
//  }
//
//    /* Streaming API */
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void firehose(final int count) {
//    ensureAuthorizationEnabled();
//    ensureStatusStreamListenerIsSet();
//    startHandler(new TwitterStreamConsumer(statusListeners, rawStreamListeners) {
//      @Override
//      public StatusStream getStream() throws TwitterException
//      {
//        return getFirehoseStream(count);
//      }
//    });
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public StatusStream getFirehoseStream(int count) throws TwitterException {
//    ensureAuthorizationEnabled();
//    return getCountStream("statuses/firehose.json", count);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void links(final int count) {
//    ensureAuthorizationEnabled();
//    ensureStatusStreamListenerIsSet();
//    startHandler(new TwitterStreamConsumer(statusListeners, rawStreamListeners) {
//      @Override
//      public StatusStream getStream() throws TwitterException {
//        return getLinksStream(count);
//      }
//    });
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public StatusStream getLinksStream(int count) throws TwitterException {
//    ensureAuthorizationEnabled();
//    return getCountStream("statuses/links.json", count);
//  }
//
//  private StatusStream getCountStream(String relativeUrl, int count) throws TwitterException {
//    ensureAuthorizationEnabled();
//    try {
//      return new StatusStreamImpl(getDispatcher(), http.post(conf.getStreamBaseURL() + relativeUrl
//          , new HttpParameter[]{new HttpParameter("count", String.valueOf(count))
//          , stallWarningsParam}, auth), conf);
//    } catch (IOException e) {
//      throw new TwitterException(e);
//    }
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void retweet() {
//    ensureAuthorizationEnabled();
//    ensureStatusStreamListenerIsSet();
//    startHandler(new TwitterStreamConsumer(statusListeners, rawStreamListeners) {
//      @Override
//      public StatusStream getStream() throws TwitterException {
//        return getRetweetStream();
//      }
//    });
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public StatusStream getRetweetStream() throws TwitterException {
//    ensureAuthorizationEnabled();
//    try {
//      return new StatusStreamImpl(getDispatcher(), http.post(conf.getStreamBaseURL() + "statuses/retweet.json"
//          , new HttpParameter[]{stallWarningsParam}, auth), conf);
//    } catch (IOException e) {
//      throw new TwitterException(e);
//    }
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void sample() {
//    ensureAuthorizationEnabled();
//    ensureStatusStreamListenerIsSet();
//    startHandler(new TwitterStreamConsumer(statusListeners, rawStreamListeners) {
//      @Override
//      public StatusStream getStream() throws TwitterException {
//        return getSampleStream();
//      }
//    });
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public StatusStream getSampleStream() throws TwitterException {
//    ensureAuthorizationEnabled();
//    try {
//      return new StatusStreamImpl(getDispatcher(), http.get(conf.getStreamBaseURL() + "statuses/sample.json?"
//                                                            + stallWarningsGetParam, auth), conf);
//    } catch (IOException e) {
//      throw new TwitterException(e);
//    }
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  public void user() {
//    user(null);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void user(final String[] track) {
//    ensureAuthorizationEnabled();
//    ensureUserStreamListenerIsSet();
//    startHandler(new TwitterStreamConsumer(statusListeners, rawStreamListeners) {
//      @Override
//      public StatusStream getStream() throws TwitterException {
//        return getUserStream(track);
//      }
//    });
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public UserStream getUserStream() throws TwitterException {
//    return getUserStream(null);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public UserStream getUserStream(String[] track) throws TwitterException {
//    ensureAuthorizationEnabled();
//    try {
//      List<HttpParameter> params = new ArrayList<HttpParameter>();
//      params.add(stallWarningsParam);
//      if (conf.isUserStreamRepliesAllEnabled()) {
//        params.add(new HttpParameter("replies", "all"));
//      }
//      if (track != null) {
//        params.add(new HttpParameter("track", z_T4JInternalStringUtil.join(track)));
//      }
//      return new UserStreamImpl(getDispatcher(), http.post(conf.getUserStreamBaseURL() + "user.json"
//          , params.toArray(new HttpParameter[params.size()])
//          , auth), conf);
//    } catch (IOException e) {
//      throw new TwitterException(e);
//    }
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public StreamController site(final boolean withFollowings, final long[] follow) {
//    ensureOAuthEnabled();
//    ensureSiteStreamsListenerIsSet();
//    final StreamController cs = new StreamController(http, auth);
//    startHandler(new TwitterStreamConsumer(siteStreamsListeners, rawStreamListeners) {
//      @Override
//      public StatusStream getStream() throws TwitterException {
//        try {
//          return new SiteStreamsImpl(getDispatcher(), getSiteStream(withFollowings, follow), conf, cs);
//        } catch (IOException e) {
//          throw new TwitterException(e);
//        }
//      }
//    });
//    return cs;
//  }
//
//  private Dispatcher getDispatcher() {
//    if (null == TwitterStreamImpl.dispatcher) {
//      synchronized (TwitterStreamImpl.class) {
//        if (null == TwitterStreamImpl.dispatcher) {
//          // dispatcher is held statically, but it'll be instantiated with
//          // the configuration instance associated with this TwitterStream
//          // instance which invokes getDispatcher() on the first time.
//          TwitterStreamImpl.dispatcher = new DispatcherFactory(conf).getInstance();
//        }
//      }
//    }
//    return TwitterStreamImpl.dispatcher;
//  }
//
//  private static transient Dispatcher dispatcher;
//
//  InputStream getSiteStream(boolean withFollowings, long[] follow) throws TwitterException {
//    ensureOAuthEnabled();
//    return http.post(conf.getSiteStreamBaseURL() + "site.json",
//                     new HttpParameter[]{
//                         new HttpParameter("with", withFollowings ? "followings" : "user")
//                         , new HttpParameter("follow", z_T4JInternalStringUtil.join(follow))
//                         , stallWarningsParam}, auth).asStream();
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void filter(final FilterQuery query) {
//    ensureAuthorizationEnabled();
//    ensureStatusStreamListenerIsSet();
//    startHandler(new TwitterStreamConsumer(statusListeners, rawStreamListeners) {
//      @Override
//      public StatusStream getStream() throws TwitterException {
//        return getFilterStream(query);
//      }
//    });
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public StatusStream getFilterStream(FilterQuery query) throws TwitterException {
//    ensureAuthorizationEnabled();
//    try {
//      return new StatusStreamImpl(getDispatcher(), http.post(conf.getStreamBaseURL()
//                                                             + "statuses/filter.json"
//          , query.asHttpParameterArray(stallWarningsParam), auth), conf);
//    } catch (IOException e) {
//      throw new TwitterException(e);
//    }
//  }
//
//
//  /**
//   * check if any listener is set. Throws IllegalStateException if no listener is set.
//   *
//   * @throws IllegalStateException when no listener is set.
//   */
//
//  private void ensureStatusStreamListenerIsSet() {
//    if (statusListeners.size() == 0 && rawStreamListeners.size() == 0) {
//      throw new IllegalStateException("StatusListener is not set.");
//    }
//  }
//
//  private void ensureUserStreamListenerIsSet() {
//    if (userStreamListeners.size() == 0 && rawStreamListeners.size() == 0) {
//      throw new IllegalStateException("UserStreamListener is not set.");
//    }
//  }
//
//  private void ensureSiteStreamsListenerIsSet() {
//    if (siteStreamsListeners.size() == 0 && rawStreamListeners.size() == 0) {
//      throw new IllegalStateException("SiteStreamsListener is not set.");
//    }
//  }
//
//  private static int numberOfHandlers = 0;
//
//  private synchronized void startHandler(TwitterStreamConsumer handler) {
//    cleanUp();
//    this.handler = handler;
//    this.handler.start();
//    numberOfHandlers++;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public synchronized void cleanUp() {
//    if (handler != null) {
//      handler.close();
//      numberOfHandlers--;
//    }
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public synchronized void shutdown() {
//    super.shutdown();
//    cleanUp();
//    synchronized (TwitterStreamImpl.class) {
//      if (0 == numberOfHandlers) {
//        if (dispatcher != null) {
//          dispatcher.shutdown();
//          dispatcher = null;
//        }
//      }
//    }
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void addConnectionLifeCycleListener(ConnectionLifeCycleListener listener) {
//    this.lifeCycleListeners.add(listener);
//  }
//
//  private List<StreamListener> userStreamListeners = new ArrayList<StreamListener>(0);
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void addListener(UserStreamListener listener) {
//    statusListeners.add(listener);
//    userStreamListeners.add(listener);
//  }
//
//  private List<StreamListener> statusListeners = new ArrayList<StreamListener>(0);
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void addListener(StatusListener listener) {
//    statusListeners.add(listener);
//  }
//
//  private List<StreamListener> siteStreamsListeners = new ArrayList<StreamListener>(0);
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void addListener(SiteStreamsListener listener) {
//    siteStreamsListeners.add(listener);
//  }
//
//  private List<RawStreamListener> rawStreamListeners = new ArrayList<RawStreamListener>(0);
//
//  /**
//   * {@inheritDoc}
//   */
//  public void addListener(RawStreamListener listener) {
//    rawStreamListeners.add(listener);
//  }
//
//  /*
//   https://dev.twitter.com/docs/streaming-api/concepts#connecting
//   When a network error (TCP/IP level) is encountered, back off linearly. Perhaps start at 250 milliseconds, double, and cap at 16 seconds
//   When a HTTP error (> 200) is returned, back off exponentially.
//   Perhaps start with a 10 second wait, double on each subsequent failure, and finally cap the wait at 240 seconds. Consider sending an alert to a human operator after multiple HTTP errors, as there is probably a client configuration issue that is unlikely to be resolved without human intervention. There's not much point in polling any faster in the face of HTTP error codes and your client is may run afoul of a rate limit.
//   */
//  private static final int TCP_ERROR_INITIAL_WAIT = 250;
//  private static final int TCP_ERROR_WAIT_CAP = 16 * 1000;
//
//  private static final int HTTP_ERROR_INITIAL_WAIT = 10 * 1000;
//  private static final int HTTP_ERROR_WAIT_CAP = 240 * 1000;
//
//  private static final int NO_WAIT = 0;
//
//  static int count = 0;
//
//  abstract class TwitterStreamConsumer extends Thread {
//    private StatusStreamBase stream = null;
//    private final String NAME = "Twitter Stream consumer-" + (++count);
//    private volatile boolean closed = false;
//    private final StreamListener[] streamListeners;
//    private final RawStreamListener[] rawStreamListeners;
//
//    TwitterStreamConsumer(List<StreamListener> streamListeners, List<RawStreamListener> rawStreamListeners) {
//      super();
//      setName(NAME + "[initializing]");
//      this.streamListeners = streamListeners.toArray(new StreamListener[streamListeners.size()]);
//      this.rawStreamListeners = rawStreamListeners.toArray(new RawStreamListener[rawStreamListeners.size()]);
//    }
//
//    @Override
//    public void run() {
//      int timeToSleep = NO_WAIT;
//      boolean connected = false;
//      while (!closed) {
//        try {
//          if (!closed && null == stream) {
//            // try establishing connection
//            logger.info("Establishing connection.");
//            setStatus("[Establishing connection]");
//            stream = (StatusStreamBase) getStream();
//            connected = true;
//            logger.info("Connection established.");
//            for (ConnectionLifeCycleListener listener : lifeCycleListeners) {
//              try {
//                listener.onConnect();
//              } catch (Exception e) {
//                logger.warn(e.getMessage());
//              }
//            }
//            // connection established successfully
//            timeToSleep = NO_WAIT;
//            logger.info("Receiving status stream.");
//            setStatus("[Receiving stream]");
//            while (!closed) {
//              try {
//                stream.next(this.streamListeners, this.rawStreamListeners);
//              } catch (IllegalStateException ise) {
//                logger.warn(ise.getMessage());
//                break;
//              } catch (TwitterException e) {
//                logger.info(e.getMessage());
//                stream.onException(e, this.streamListeners, this.rawStreamListeners);
//                throw e;
//              } catch (Exception e) {
//                logger.info(e.getMessage());
//                stream.onException(e, this.streamListeners, this.rawStreamListeners);
//                closed = true;
//                break;
//              }
//            }
//          }
//        } catch (TwitterException te) {
//          logger.info(te.getMessage());
//          if (!closed) {
//            if (NO_WAIT == timeToSleep) {
//              if (te.getStatusCode() == FORBIDDEN) {
//                logger.warn("This account is not in required role. ", te.getMessage());
//                closed = true;
//                for (StreamListener statusListener : streamListeners) {
//                  statusListener.onException(te);
//                }
//                break;
//              }
//              if (te.getStatusCode() == NOT_ACCEPTABLE) {
//                logger.warn("Parameter not accepted with the role. ", te.getMessage());
//                closed = true;
//                for (StreamListener statusListener : streamListeners) {
//                  statusListener.onException(te);
//                }
//                break;
//              }
//              connected = false;
//              for (ConnectionLifeCycleListener listener : lifeCycleListeners) {
//                try {
//                  listener.onDisconnect();
//                } catch (Exception e) {
//                  logger.warn(e.getMessage());
//                }
//              }
//              if (te.getStatusCode() > 200) {
//                timeToSleep = HTTP_ERROR_INITIAL_WAIT;
//              } else if (0 == timeToSleep) {
//                timeToSleep = TCP_ERROR_INITIAL_WAIT;
//              }
//            }
//            if (te.getStatusCode() > 200 && timeToSleep < HTTP_ERROR_INITIAL_WAIT) {
//              timeToSleep = HTTP_ERROR_INITIAL_WAIT;
//            }
//            if (connected) {
//              for (ConnectionLifeCycleListener listener : lifeCycleListeners) {
//                try {
//                  listener.onDisconnect();
//                } catch (Exception e) {
//                  logger.warn(e.getMessage());
//                }
//              }
//            }
//            for (StreamListener statusListener : streamListeners) {
//              statusListener.onException(te);
//            }
//            // there was a problem establishing the connection, or the connection closed by peer
//            if (!closed) {
//              // wait for a moment not to overload Twitter API
//              logger.info("Waiting for " + (timeToSleep) + " milliseconds");
//              setStatus("[Waiting for " + (timeToSleep) + " milliseconds]");
//              try {
//                Thread.sleep(timeToSleep);
//              } catch (InterruptedException ignore) {
//              }
//              timeToSleep = Math.min(timeToSleep * 2, (te.getStatusCode() > 200) ? HTTP_ERROR_WAIT_CAP : TCP_ERROR_WAIT_CAP);
//            }
//            stream = null;
//            logger.debug(te.getMessage());
//            connected = false;
//          }
//        }
//      }
//      if (this.stream != null && connected) {
//        try {
//          this.stream.close();
//        } catch (IOException ignore) {
//        } catch (Exception e) {
//          e.printStackTrace();
//          logger.warn(e.getMessage());
//        } finally {
//          for (ConnectionLifeCycleListener listener : lifeCycleListeners) {
//            try {
//              listener.onDisconnect();
//            } catch (Exception e) {
//              logger.warn(e.getMessage());
//            }
//          }
//        }
//      }
//      for (ConnectionLifeCycleListener listener : lifeCycleListeners) {
//        try {
//          listener.onCleanUp();
//        } catch (Exception e) {
//          logger.warn(e.getMessage());
//        }
//      }
//    }
//
//    public synchronized void close() {
//      setStatus("[Disposing thread]");
//      try {
//        if (stream != null) {
//          try {
//            stream.close();
//          } catch (IOException ignore) {
//          } catch (Exception e) {
//            e.printStackTrace();
//            logger.warn(e.getMessage());
//          }
//        }
//      } finally {
//        closed = true;
//      }
//    }
//
//    private void setStatus(String message) {
//      String actualMessage = NAME + message;
//      setName(actualMessage);
//      logger.debug(actualMessage);
//    }
//
//    abstract StatusStream getStream() throws TwitterException;
//
//  }
//
//  @Override
//  public boolean equals(Object o) {
//    if (this == o) return true;
//    if (o == null || getClass() != o.getClass()) return false;
//    if (!super.equals(o)) return false;
//
//    TwitterStreamImpl that = (TwitterStreamImpl) o;
//
//    if (handler != null ? !handler.equals(that.handler) : that.handler != null) return false;
//    if (http != null ? !http.equals(that.http) : that.http != null) return false;
//    if (lifeCycleListeners != null ? !lifeCycleListeners.equals(that.lifeCycleListeners) : that.lifeCycleListeners != null)
//      return false;
//    if (rawStreamListeners != null ? !rawStreamListeners.equals(that.rawStreamListeners) : that.rawStreamListeners != null)
//      return false;
//    if (siteStreamsListeners != null ? !siteStreamsListeners.equals(that.siteStreamsListeners) : that.siteStreamsListeners != null)
//      return false;
//    if (stallWarningsGetParam != null ? !stallWarningsGetParam.equals(that.stallWarningsGetParam) : that.stallWarningsGetParam != null)
//      return false;
//    if (stallWarningsParam != null ? !stallWarningsParam.equals(that.stallWarningsParam) : that.stallWarningsParam != null)
//      return false;
//    if (statusListeners != null ? !statusListeners.equals(that.statusListeners) : that.statusListeners != null)
//      return false;
//    if (userStreamListeners != null ? !userStreamListeners.equals(that.userStreamListeners) : that.userStreamListeners != null)
//      return false;
//
//    return true;
//  }
//
//  @Override
//  public int hashCode() {
//    int result = super.hashCode();
//    result = 31 * result + (http != null ? http.hashCode() : 0);
//    result = 31 * result + (lifeCycleListeners != null ? lifeCycleListeners.hashCode() : 0);
//    result = 31 * result + (handler != null ? handler.hashCode() : 0);
//    result = 31 * result + (stallWarningsGetParam != null ? stallWarningsGetParam.hashCode() : 0);
//    result = 31 * result + (stallWarningsParam != null ? stallWarningsParam.hashCode() : 0);
//    result = 31 * result + (userStreamListeners != null ? userStreamListeners.hashCode() : 0);
//    result = 31 * result + (statusListeners != null ? statusListeners.hashCode() : 0);
//    result = 31 * result + (siteStreamsListeners != null ? siteStreamsListeners.hashCode() : 0);
//    result = 31 * result + (rawStreamListeners != null ? rawStreamListeners.hashCode() : 0);
//    return result;
//  }
//
//  @Override
//  public String toString() {
//    return "TwitterStreamImpl{" +
//           "http=" + http +
//           ", lifeCycleListeners=" + lifeCycleListeners +
//           ", handler=" + handler +
//           ", stallWarningsGetParam='" + stallWarningsGetParam + '\'' +
//           ", stallWarningsParam=" + stallWarningsParam +
//           ", userStreamListeners=" + userStreamListeners +
//           ", statusListeners=" + statusListeners +
//           ", siteStreamsListeners=" + siteStreamsListeners +
//           ", rawStreamListeners=" + rawStreamListeners +
//           '}';
//  }
//}
//
//class StreamingReadTimeoutConfiguration implements HttpClientWrapperConfiguration
//{
//  twitter4j.conf.Configuration nestedConf;
//
//  StreamingReadTimeoutConfiguration(twitter4j.conf.Configuration httpConf) {
//    this.nestedConf = httpConf;
//  }
//
//  @Override
//  public String getHttpProxyHost() {
//    return nestedConf.getHttpProxyHost();
//  }
//
//  @Override
//  public int getHttpProxyPort() {
//    return nestedConf.getHttpProxyPort();
//  }
//
//  @Override
//  public String getHttpProxyUser() {
//    return nestedConf.getHttpProxyUser();
//  }
//
//  @Override
//  public String getHttpProxyPassword() {
//    return nestedConf.getHttpProxyPassword();
//  }
//
//  @Override
//  public int getHttpConnectionTimeout() {
//    return nestedConf.getHttpConnectionTimeout();
//  }
//
//  @Override
//  public int getHttpReadTimeout() {
//    // this is the trick that overrides connection timeout
//    return nestedConf.getHttpStreamingReadTimeout();
//  }
//
//  @Override
//  public int getHttpRetryCount() {
//    return nestedConf.getHttpRetryCount();
//  }
//
//  @Override
//  public int getHttpRetryIntervalSeconds() {
//    return nestedConf.getHttpRetryIntervalSeconds();
//  }
//
//  @Override
//  public int getHttpMaxTotalConnections() {
//    return nestedConf.getHttpMaxTotalConnections();
//  }
//
//  @Override
//  public int getHttpDefaultMaxPerRoute() {
//    return nestedConf.getHttpDefaultMaxPerRoute();
//  }
//
//  @Override
//  public Map<String, String> getRequestHeaders() {
//    // turning off keepalive connection explicitly because Streaming API doesn't need keepalive connection.
//    // and this will reduce the shutdown latency of streaming api connection
//    // see also - http://jira.twitter4j.org/browse/TFJ-556
//    Map<String, String> headers = new HashMap<String, String>(nestedConf.getRequestHeaders());
//    headers.put("Connection", "close");
//    return headers;
//  }
//
//  @Override
//  public boolean isPrettyDebugEnabled() {
//    return nestedConf.isPrettyDebugEnabled();
//  }
//
//  @Override
//  public boolean isGZIPEnabled() {
//    return nestedConf.isGZIPEnabled();
//  }
//}
//
