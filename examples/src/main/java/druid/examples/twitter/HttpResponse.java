package druid.examples.twitter;

/*
* Copyright 2007 Yusuke Yamamoto
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import org.codehaus.jettison.json.JSONException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
* A data class representing HTTP Response
*
* @author Yusuke Yamamoto - yusuke at mac.com
*/
public abstract class HttpResponse {
  private static final Logger logger = Logger.getLogger(HttpResponseImpl.class);
  protected final HttpClientConfiguration CONF;

  HttpResponse() {
    this.CONF = ConfigurationContext.getInstance();
  }

  public HttpResponse(HttpClientConfiguration conf) {
    this.CONF = conf;
  }

  protected int statusCode;
  protected String responseAsString = null;
  protected InputStream is;
  private boolean streamConsumed = false;

  public int getStatusCode() {
    return statusCode;
  }

  public abstract String getResponseHeader(String name);

  public abstract Map<String, List<String>> getResponseHeaderFields();

  /**
   * Returns the response stream.<br>
   * This method cannot be called after calling asString() or asDcoument()<br>
   * It is suggested to call disconnect() after consuming the stream.
   * <p/>
   * Disconnects the internal HttpURLConnection silently.
   *
   * @return response body stream
   * @see #disconnect()
   */
  public InputStream asStream() {
    if (streamConsumed) {
      throw new IllegalStateException("Stream has already been consumed.");
    }
    return is;
  }

  /**
   * Returns the response body as string.<br>
   * Disconnects the internal HttpURLConnection silently.
   *
   * @return response body
   */
  public String asString() throws RuntimeException {
    if (null == responseAsString) {
      BufferedReader br = null;
      InputStream stream = null;
      try {
        stream = asStream();
        if (null == stream) {
          return null;
        }
        br = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
        StringBuilder buf = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
          buf.append(line).append("\n");
        }
        this.responseAsString = buf.toString();
        //logger.debug(responseAsString);
        stream.close();
        streamConsumed = true;
      } catch (IOException ioe) {
        throw new RuntimeException(ioe.getMessage(), ioe);
      } finally {
        if (stream != null) {
          try {
            stream.close();
          } catch (IOException ignore) {
          }
        }
        if (br != null) {
          try {
            br.close();
          } catch (IOException ignore) {
          }
        }
        disconnectForcibly();
      }
    }
    return responseAsString;
  }

  private JSONObject json = null;

  /**
   * Returns the response body as twitter4j.internal.org.json.JSONObject.<br>
   * Disconnects the internal HttpURLConnection silently.
   *
   * @return response body as JSONObject
   */
  public JSONObject asJSONObject() throws RuntimeException {
    if (json == null) {
      Reader reader = null;
      try {
        if (responseAsString == null) {
          reader = asReader();
          json = new JSONObject(new JSONTokener(reader));
        } else {
          json = new JSONObject(responseAsString);
        }
        if (CONF.isPrettyDebugEnabled()) {
          //logger.debug(json.toString(1));
        } else {
          //logger.debug(responseAsString != null ? responseAsString :
//                       json.toString());
        }
      } catch (JSONException jsone) {
        if (responseAsString == null) {
          throw new RuntimeException(jsone.getMessage(), jsone);
        } else {
          throw new RuntimeException(jsone.getMessage() + ":" + this.responseAsString, jsone);
        }
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ignore) {
          }
        }
        disconnectForcibly();
      }
    }
    return json;
  }

  private JSONArray jsonArray = null;

  /**
   * Returns the response body as twitter4j.internal.org.json.JSONArray.<br>
   * Disconnects the internal HttpURLConnection silently.
   *
   * @return response body as twitter4j.internal.org.json.JSONArray
   * @throws RuntimeException
   */
  public JSONArray asJSONArray() throws RuntimeException {
    if (jsonArray == null) {
      Reader reader = null;
      try {
        if (responseAsString == null) {
          reader = asReader();
          jsonArray = new JSONArray(new JSONTokener(reader));
        } else {
          jsonArray = new JSONArray(responseAsString);
        }
        if (CONF.isPrettyDebugEnabled()) {
          //logger.debug(jsonArray.toString(1));
        } else {
          //logger.debug(responseAsString != null ? responseAsString :
//                       jsonArray.toString());
        }
      } catch (JSONException jsone) {
//        if (logger.isDebugEnabled()) {
//          throw new RuntimeException(jsone.getMessage() + ":" + this.responseAsString, jsone);
//        } else {
//          throw new RuntimeException(jsone.getMessage(), jsone);
//        }
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ignore) {
          }
        }
        disconnectForcibly();
      }
    }
    return jsonArray;
  }

  public Reader asReader() {
    try {
      return new BufferedReader(new InputStreamReader(is, "UTF-8"));
    } catch (java.io.UnsupportedEncodingException uee) {
      return new InputStreamReader(is);
    }
  }

  private void disconnectForcibly() {
    try {
      disconnect();
    } catch (Exception ignore) {
    }
  }

  public abstract void disconnect() throws IOException;

  @Override
  public String toString() {
    return "HttpResponse{" +
           "statusCode=" + statusCode +
           ", responseAsString='" + responseAsString + '\'' +
           ", is=" + is +
           ", streamConsumed=" + streamConsumed +
           '}';
  }
}