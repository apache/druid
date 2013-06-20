package druid.examples.twitter;
/**
 * Created with IntelliJ IDEA.
 * User: dhruvparthasarathy
 * Date: 6/19/13
 * Time: 2:02 PM
 * To change this template use File | Settings | File Templates.
 */

import druid.examples.twitter.Configuration;
import twitter4j.internal.util.z_T4JInternalStringUtil;

import java.io.ObjectStreamException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


import java.io.ObjectStreamException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.logging.Logger;

/**
 * Configuration base class with default settings.
 *
 * @author Yusuke Yamamoto - yusuke at mac.com
 */
class WebConfigurationBase implements Configuration, java.io.Serializable {
  private boolean debug;
  private boolean useSSL;
  private boolean prettyDebug;
  private boolean gzipEnabled;
  private String httpProxyHost;
  private String httpProxyUser;
  private String httpProxyPassword;
  private String userAgent;
  private int httpProxyPort;
  private int httpConnectionTimeout;
  private int httpReadTimeout;

  private int httpStreamingReadTimeout;
  private int httpRetryCount;
  private int httpRetryIntervalSeconds;
  private int maxTotalConnections;
  private int defaultMaxPerRoute;

  private String streamBaseURL;

  private String dispatcherImpl;
  private String loggerFactory;

  private int asyncNumThreads;

  private long contributingTo;
  private boolean includeEntitiesEnabled = true;

  private boolean jsonStoreEnabled;


  private boolean stallWarningsEnabled;



  private static final String DEFAULT_STREAM_BASE_URL = "http://developer.usa.gov/1usagov/";

  private static final long serialVersionUID = -6610497517837844232L;


  protected WebConfigurationBase() {
    setDebug(false);
    setUseSSL(false);
    setPrettyDebugEnabled(false);
    setGZIPEnabled(true);
    setHttpProxyHost(null);
    setHttpProxyUser(null);
    setHttpProxyPassword(null);
    setUserAgent(null);
    setHttpProxyPort(-1);
    setHttpConnectionTimeout(20000);
    setHttpReadTimeout(120000);
    setHttpStreamingReadTimeout(40 * 1000);
    setHttpRetryCount(0);
    setHttpRetryIntervalSeconds(5);
    setHttpMaxTotalConnections(20);
    setHttpDefaultMaxPerRoute(2);
    setAsyncNumThreads(1);
    setContributingTo(-1L);

    setJSONStoreEnabled(false);


    setStreamBaseURL(DEFAULT_STREAM_BASE_URL);

    setDispatcherImpl("twitter4j.internal.async.DispatcherImpl");
    setLoggerFactory(null);

    setStallWarningsEnabled(true);

  }

  public void dumpConfiguration() {
    if (debug) {
//      Field[] fields = WebConfigurationBase.class.getDeclaredFields();
//      for (Field field : fields) {
//        try {
//          Object value = field.get(this);
//          String strValue = String.valueOf(value);
//          if (value != null && field.getName().matches("oAuthConsumerSecret|oAuthAccessTokenSecret|password")) {
//            strValue = z_T4JInternalStringUtil.maskString(String.valueOf(value));
//          }
//          log.debug(field.getName() + ": " + strValue);
//        } catch (IllegalAccessException ignore) {
//        }
//      }
    }
  }

  @Override
  public final boolean isDebugEnabled() {
    return debug;
  }

  protected final void setDebug(boolean debug) {
    this.debug = debug;
  }


  @Override
  public boolean isPrettyDebugEnabled() {
    return prettyDebug;
  }

  protected final void setUseSSL(boolean useSSL) {
    this.useSSL = useSSL;
  }

  protected final void setPrettyDebugEnabled(boolean prettyDebug) {
    this.prettyDebug = prettyDebug;
  }

  protected final void setGZIPEnabled(boolean gzipEnabled) {
    this.gzipEnabled = gzipEnabled;
    initRequestHeaders();
  }

  @Override
  public boolean isGZIPEnabled() {
    return gzipEnabled;
  }

  // method for HttpRequestFactoryConfiguration
  Map<String, String> requestHeaders;

  private void initRequestHeaders() {
//    requestHeaders = new HashMap<String, String>();
//    requestHeaders.put("X-Twitter-Client-Version", getClientVersion());
//    requestHeaders.put("X-Twitter-Client-URL", getClientURL());
//    requestHeaders.put("X-Twitter-Client", "Twitter4J");
//
//    requestHeaders.put("User-Agent", getUserAgent());
//    if (gzipEnabled) {
//      requestHeaders.put("Accept-Encoding", "gzip");
//    }
//    if (IS_DALVIK) {
//      requestHeaders.put("Connection", "close");
//    }
  }

  @Override
  public Map<String, String> getRequestHeaders() {
    return requestHeaders;
  }

  // methods for HttpClientConfiguration

  @Override
  public final String getHttpProxyHost() {
    return httpProxyHost;
  }

  protected final void setHttpProxyHost(String proxyHost) {
    this.httpProxyHost = proxyHost;
  }

  @Override
  public final String getHttpProxyUser() {
    return httpProxyUser;
  }

  protected final void setHttpProxyUser(String proxyUser) {
    this.httpProxyUser = proxyUser;
  }

  @Override
  public final String getHttpProxyPassword() {
    return httpProxyPassword;
  }

  protected final void setHttpProxyPassword(String proxyPassword) {
    this.httpProxyPassword = proxyPassword;
  }

  @Override
  public final int getHttpProxyPort() {
    return httpProxyPort;
  }

  protected final void setHttpProxyPort(int proxyPort) {
    this.httpProxyPort = proxyPort;
  }

  @Override
  public final int getHttpConnectionTimeout() {
    return httpConnectionTimeout;
  }

  protected final void setHttpConnectionTimeout(int connectionTimeout) {
    this.httpConnectionTimeout = connectionTimeout;
  }

  @Override
  public final int getHttpReadTimeout() {
    return httpReadTimeout;
  }

  protected final void setHttpReadTimeout(int readTimeout) {
    this.httpReadTimeout = readTimeout;
  }

  @Override
  public int getHttpStreamingReadTimeout() {
    return httpStreamingReadTimeout;
  }

  protected final void setHttpStreamingReadTimeout(int httpStreamingReadTimeout) {
    this.httpStreamingReadTimeout = httpStreamingReadTimeout;
  }


  @Override
  public final int getHttpRetryCount() {
    return httpRetryCount;
  }

  protected final void setHttpRetryCount(int retryCount) {
    this.httpRetryCount = retryCount;
  }

  @Override
  public final int getHttpRetryIntervalSeconds() {
    return httpRetryIntervalSeconds;
  }

  protected final void setHttpRetryIntervalSeconds(int retryIntervalSeconds) {
    this.httpRetryIntervalSeconds = retryIntervalSeconds;
  }

  @Override
  public final int getHttpMaxTotalConnections() {
    return maxTotalConnections;
  }

  protected final void setHttpMaxTotalConnections(int maxTotalConnections) {
    this.maxTotalConnections = maxTotalConnections;
  }

  @Override
  public final int getHttpDefaultMaxPerRoute() {
    return defaultMaxPerRoute;
  }

  protected final void setHttpDefaultMaxPerRoute(int defaultMaxPerRoute) {
    this.defaultMaxPerRoute = defaultMaxPerRoute;
  }


  @Override
  public final int getAsyncNumThreads() {
    return asyncNumThreads;
  }

  protected final void setAsyncNumThreads(int asyncNumThreads) {
    this.asyncNumThreads = asyncNumThreads;
  }

  @Override
  public final long getContributingTo() {
    return contributingTo;
  }

  protected final void setContributingTo(long contributingTo) {
    this.contributingTo = contributingTo;
  }


  @Override
  public String getStreamBaseURL() {
    return streamBaseURL;
  }

  protected final void setStreamBaseURL(String streamBaseURL) {
    this.streamBaseURL = streamBaseURL;
  }


  @Override
  public String getDispatcherImpl() {
    return dispatcherImpl;
  }

  protected final void setDispatcherImpl(String dispatcherImpl) {
    this.dispatcherImpl = dispatcherImpl;
  }

  protected final void setUserAgent(String userAgent) {
    this.userAgent= userAgent;
  }

  @Override
  public String getLoggerFactory() {
    return loggerFactory;
  }


  protected final void setLoggerFactory(String loggerImpl) {
    this.loggerFactory = loggerImpl;
  }


  public boolean isJSONStoreEnabled() {
    return this.jsonStoreEnabled;
  }

  protected final void setJSONStoreEnabled(boolean enabled) {
    this.jsonStoreEnabled = enabled;
  }

  @Override
  public boolean isStallWarningsEnabled() {
    return stallWarningsEnabled;
  }

  protected final void setStallWarningsEnabled(boolean stallWarningsEnabled) {
    this.stallWarningsEnabled = stallWarningsEnabled;
  }

  static String fixURL(boolean useSSL, String url) {
    if (null == url) {
      return null;
    }
    int index = url.indexOf("://");
    if (-1 == index) {
      throw new IllegalArgumentException("url should contain '://'");
    }
    String hostAndLater = url.substring(index + 3);
    if (useSSL) {
      return "https://" + hostAndLater;
    } else {
      return "http://" + hostAndLater;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    WebConfigurationBase that = (WebConfigurationBase) o;

    if (asyncNumThreads != that.asyncNumThreads) return false;
    if (contributingTo != that.contributingTo) return false;
    if (debug != that.debug) return false;
    if (defaultMaxPerRoute != that.defaultMaxPerRoute) return false;
    if (gzipEnabled != that.gzipEnabled) return false;
    if (httpConnectionTimeout != that.httpConnectionTimeout) return false;
    if (httpProxyPort != that.httpProxyPort) return false;
    if (httpReadTimeout != that.httpReadTimeout) return false;
    if (httpRetryCount != that.httpRetryCount) return false;
    if (httpRetryIntervalSeconds != that.httpRetryIntervalSeconds) return false;
    if (httpStreamingReadTimeout != that.httpStreamingReadTimeout) return false;
    if (includeEntitiesEnabled != that.includeEntitiesEnabled) return false;
    if (jsonStoreEnabled != that.jsonStoreEnabled) return false;
    if (maxTotalConnections != that.maxTotalConnections) return false;
    if (prettyDebug != that.prettyDebug) return false;
    if (stallWarningsEnabled != that.stallWarningsEnabled) return false;
    if (useSSL != that.useSSL) return false;
    if (dispatcherImpl != null ? !dispatcherImpl.equals(that.dispatcherImpl) : that.dispatcherImpl != null)
      return false;
    if (httpProxyHost != null ? !httpProxyHost.equals(that.httpProxyHost) : that.httpProxyHost != null)
      return false;
    if (httpProxyPassword != null ? !httpProxyPassword.equals(that.httpProxyPassword) : that.httpProxyPassword != null)
      return false;
    if (httpProxyUser != null ? !httpProxyUser.equals(that.httpProxyUser) : that.httpProxyUser != null)
      return false;
    if (loggerFactory != null ? !loggerFactory.equals(that.loggerFactory) : that.loggerFactory != null)
      return false;
    if (requestHeaders != null ? !requestHeaders.equals(that.requestHeaders) : that.requestHeaders != null)
      return false;
    if (streamBaseURL != null ? !streamBaseURL.equals(that.streamBaseURL) : that.streamBaseURL != null)
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = (debug ? 1 : 0);
    result = 31 * result + (useSSL ? 1 : 0);
    result = 31 * result + (prettyDebug ? 1 : 0);
    result = 31 * result + (gzipEnabled ? 1 : 0);
    result = 31 * result + (httpProxyHost != null ? httpProxyHost.hashCode() : 0);
    result = 31 * result + (httpProxyUser != null ? httpProxyUser.hashCode() : 0);
    result = 31 * result + (httpProxyPassword != null ? httpProxyPassword.hashCode() : 0);
    result = 31 * result + httpProxyPort;
    result = 31 * result + httpConnectionTimeout;
    result = 31 * result + httpReadTimeout;
    result = 31 * result + httpStreamingReadTimeout;
    result = 31 * result + httpRetryCount;
    result = 31 * result + httpRetryIntervalSeconds;
    result = 31 * result + maxTotalConnections;
    result = 31 * result + defaultMaxPerRoute;
    result = 31 * result + (streamBaseURL != null ? streamBaseURL.hashCode() : 0);
    result = 31 * result + (dispatcherImpl != null ? dispatcherImpl.hashCode() : 0);
    result = 31 * result + (loggerFactory != null ? loggerFactory.hashCode() : 0);
    result = 31 * result + asyncNumThreads;
    result = 31 * result + (int) (contributingTo ^ (contributingTo >>> 32));
    result = 31 * result + (includeEntitiesEnabled ? 1 : 0);
    result = 31 * result + (jsonStoreEnabled ? 1 : 0);
    result = 31 * result + (stallWarningsEnabled ? 1 : 0);
    result = 31 * result + (requestHeaders != null ? requestHeaders.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "ConfigurationBase{" +
           "debug=" + debug +
           ", useSSL=" + useSSL +
           ", prettyDebug=" + prettyDebug +
           ", gzipEnabled=" + gzipEnabled +
           ", httpProxyHost='" + httpProxyHost + '\'' +
           ", httpProxyUser='" + httpProxyUser + '\'' +
           ", httpProxyPassword='" + httpProxyPassword + '\'' +
           ", httpProxyPort=" + httpProxyPort +
           ", httpConnectionTimeout=" + httpConnectionTimeout +
           ", httpReadTimeout=" + httpReadTimeout +
           ", httpStreamingReadTimeout=" + httpStreamingReadTimeout +
           ", httpRetryCount=" + httpRetryCount +
           ", httpRetryIntervalSeconds=" + httpRetryIntervalSeconds +
           ", maxTotalConnections=" + maxTotalConnections +
           ", defaultMaxPerRoute=" + defaultMaxPerRoute +
           ", streamBaseURL='" + streamBaseURL + '\'' +
           ", dispatcherImpl='" + dispatcherImpl + '\'' +
           ", loggerFactory='" + loggerFactory + '\'' +
           ", asyncNumThreads=" + asyncNumThreads +
           ", contributingTo=" + contributingTo +
           ", includeEntitiesEnabled=" + includeEntitiesEnabled +
           ", jsonStoreEnabled=" + jsonStoreEnabled +
           ", stallWarningsEnabled=" + stallWarningsEnabled +
           ", requestHeaders=" + requestHeaders +
           '}';
  }

  private static final List<WebConfigurationBase> instances = new ArrayList<WebConfigurationBase>();

  private static void cacheInstance(WebConfigurationBase conf) {
    if (!instances.contains(conf)) {
      instances.add(conf);
    }
  }

  protected void cacheInstance() {
    cacheInstance(this);
  }

  private static WebConfigurationBase getInstance(WebConfigurationBase configurationBase) {
    int index;
    if ((index = instances.indexOf(configurationBase)) == -1) {
      instances.add(configurationBase);
      return configurationBase;
    } else {
      return instances.get(index);
    }
  }

  // assures equality after deserializedation
  protected Object readResolve() throws ObjectStreamException
  {
    return getInstance(this);
  }
}
