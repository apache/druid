package druid.examples.twitter.HttpClientWrapperConfiguration;

import druid.examples.twitter.HttpClientConfiguration;

import java.util.Map;

public interface HttpClientWrapperConfiguration extends HttpClientConfiguration
{
  /**
   * @return request headers
   */
  Map<String, String> getRequestHeaders();
}
