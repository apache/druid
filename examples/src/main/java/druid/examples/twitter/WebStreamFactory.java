//package druid.examples.twitter;
//
///*
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
///**
// * An instance of this class is completely thread safe and can be re-used and used concurrently.<br>
// * Note that TwitterStream is NOT compatible with Google App Engine as GAE is not capable of handling requests longer than 30 seconds.
// *
// */
//public final class WebStreamFactory implements java.io.Serializable {
//  private static final long serialVersionUID = 8146074704915782233L;
//  private final Configuration conf;
//  private static final WebStream SINGLETON;
//
//  static {
//    SINGLETON = new TwitterStreamImpl(ConfigurationContext.getInstance(), TwitterFactory.DEFAULT_AUTHORIZATION);
//  }
//
//  /**
//   * Creates a TwitterStreamFactory with the root configuration.
//   */
//  public TwitterStreamFactory() {
//    this(ConfigurationContext.getInstance());
//  }
//
//  /**
//   * Creates a TwitterStreamFactory with the given configuration.
//   *
//   * @param conf the configuration to use
//   * @since Twitter4J 2.1.1
//   */
//  public WebStreamFactory(Configuration conf) {
//    this.conf = conf;
//  }
//
//  /**
//   * Creates a TwitterStreamFactory with a specified config tree.
//   *
//   * @param configTreePath the path
//   */
//  public WebStreamFactory(String configTreePath) {
//    this(ConfigurationContext.getInstance(configTreePath));
//  }
//
//  // implementations for BasicSupportFactory
//
//  /**
//   * Returns a instance associated with the configuration bound to this factory.
//   *
//   * @return default instance
//   */
//  public WebStream getInstance() {
//    return getInstance(AuthorizationFactory.getInstance(conf));
//  }
//
//  /**
//   * Returns a OAuth Authenticated instance.<br>
//   * consumer key and consumer Secret must be provided by twitter4j.properties, or system properties.
//   * Unlike {@link TwitterStream#setOAuthAccessToken(twitter4j.auth.AccessToken)}, this factory method potentially returns a cached instance.
//   *
//   * @param accessToken access token
//   * @return an instance
//   */
//  public TwitterStream getInstance(AccessToken accessToken) {
//    String consumerKey = conf.getOAuthConsumerKey();
//    String consumerSecret = conf.getOAuthConsumerSecret();
//    if (null == consumerKey && null == consumerSecret) {
//      throw new IllegalStateException("Consumer key and Consumer secret not supplied.");
//    }
//    OAuthAuthorization oauth = new OAuthAuthorization(conf);
//    oauth.setOAuthAccessToken(accessToken);
//    return getInstance(conf, oauth);
//  }
//
//  /**
//   * Returns a instance.
//   *
//   * @return an instance
//   */
//  public TwitterStream getInstance(Authorization auth) {
//    return getInstance(conf, auth);
//  }
//
//  private TwitterStream getInstance(twitter4j.conf.Configuration conf, Authorization auth) {
//    return new TwitterStreamImpl(conf, auth);
//  }
//
//  /**
//   * Returns default singleton TwitterStream instance.
//   *
//   * @return default singleton TwitterStream instance
//   * @since Twitter4J 2.2.4
//   */
//  public static TwitterStream getSingleton() {
//    return SINGLETON;
//  }
//}
