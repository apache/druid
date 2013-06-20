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
//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.util.ArrayList;
//import java.util.List;
//
//
//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * Base class of Twitter / AsyncTwitter / TwitterStream supports OAuth.
// *
// * @author Yusuke Yamamoto - yusuke at mac.com
// */
//abstract class WebBaseImpl implements WebBase, java.io.Serializable, HttpResponseListener
//{
//  protected Configuration conf;
//  protected transient String screenName = null;
//  protected transient long id = 0;
//
//  protected transient HttpClientWrapper http;
//
//  protected z_T4JInternalFactory factory;
//
//  private static final long serialVersionUID = -3812176145960812140L;
//
//  /*package*/ WebBaseImpl(Configuration conf) {
//    this.conf = conf;
//    init();
//  }
//
//  private void init() {
//
//    http = new HttpClientWrapper(conf);
//    http.setHttpResponseListener(this);
//    setFactory();
//  }
//
//  protected void setFactory() {
//    factory = new z_T4JInternalJSONImplFactory(conf);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//
//  @Override
//  public void httpResponseReceived(HttpResponseEvent event) {
//    if (rateLimitStatusListeners.size() != 0) {
//      HttpResponse res = event.getResponse();
//      TwitterException te = event.getTwitterException();
//      RateLimitStatus rateLimitStatus;
//      int statusCode;
//      if (te != null) {
//        rateLimitStatus = te.getRateLimitStatus();
//        statusCode = te.getStatusCode();
//      } else {
//        rateLimitStatus = z_T4JInternalJSONImplFactory.createRateLimitStatusFromResponseHeader(res);
//        statusCode = res.getStatusCode();
//      }
//      if (rateLimitStatus != null) {
//        RateLimitStatusEvent statusEvent
//            = new RateLimitStatusEvent(this, rateLimitStatus, event.isAuthenticated());
//        if (statusCode == ENHANCE_YOUR_CLAIM
//            || statusCode == SERVICE_UNAVAILABLE) {
//          // EXCEEDED_RATE_LIMIT_QUOTA is returned by Rest API
//          // SERVICE_UNAVAILABLE is returned by Search API
//          for (RateLimitStatusListener listener : rateLimitStatusListeners) {
//            listener.onRateLimitStatus(statusEvent);
//            listener.onRateLimitReached(statusEvent);
//          }
//        } else {
//          for (RateLimitStatusListener listener : rateLimitStatusListeners) {
//            listener.onRateLimitStatus(statusEvent);
//          }
//        }
//      }
//    }
//  }
//
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Configuration getConfiguration() {
//    return this.conf;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void shutdown() {
//    if (http != null) http.shutdown();
//  }
//
//
//  private void writeObject(java.io.ObjectOutputStream out) throws IOException
//  {
//    // http://docs.oracle.com/javase/6/docs/platform/serialization/spec/output.html#861
//    out.putFields();
//    out.writeFields();
//
//    out.writeObject(conf);
//  }
//
//  private void readObject(ObjectInputStream stream)
//      throws IOException, ClassNotFoundException {
//    // http://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html#2971
//    stream.readFields();
//
//    conf = (Configuration) stream.readObject();
//    http = new HttpClientWrapper(conf);
//    http.setHttpResponseListener(this);
//    setFactory();
//  }
//
//
//
//  @Override
//  public boolean equals(Object o) {
//    if (this == o) return true;
//    if (!(o instanceof WebBaseImpl)) return false;
//
//    WebBaseImpl that = (WebBaseImpl) o;
//
//    if (!conf.equals(that.conf)) return false;
//    if (http != null ? !http.equals(that.http) : that.http != null)
//      return false;
//
//    return true;
//  }
//
//  @Override
//  public int hashCode() {
//    int result = conf.hashCode();
//    result = 31 * result + (http != null ? http.hashCode() : 0);
//    return result;
//  }
//
//  @Override
//  public String toString() {
//    return "TwitterBase{" +
//           "conf=" + conf +
//           ", http=" + http +
//           '}';
//  }
//}
