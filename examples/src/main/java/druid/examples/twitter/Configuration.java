package druid.examples.twitter;

import druid.examples.twitter.HttpClientWrapperConfiguration.HttpClientWrapperConfiguration;

import java.util.Map;


public interface Configuration extends HttpClientConfiguration
    , HttpClientWrapperConfiguration
    , java.io.Serializable {


  boolean isDebugEnabled();


  Map<String, String> getRequestHeaders();

  // methods for HttpClientConfiguration

  String getHttpProxyHost();

  String getHttpProxyUser();

  String getHttpProxyPassword();

  int getHttpProxyPort();

  int getHttpConnectionTimeout();

  int getHttpReadTimeout();

  int getHttpStreamingReadTimeout();

  int getHttpRetryCount();

  int getHttpRetryIntervalSeconds();

  int getHttpMaxTotalConnections();

  int getHttpDefaultMaxPerRoute();


  String getStreamBaseURL();


  boolean isJSONStoreEnabled();


  boolean isStallWarningsEnabled();

  int getAsyncNumThreads();

  long getContributingTo();

  String getDispatcherImpl();

  String getLoggerFactory();

}
