/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.java.util.emitter.core;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParametrizedUriExtractor implements UriExtractor
{
  private String uriPattern;
  private Set<String> params;

  public ParametrizedUriExtractor(String uriPattern)
  {
    this.uriPattern = uriPattern;
    Matcher keyMatcher = Pattern.compile("\\{([^\\}]+)\\}").matcher(uriPattern);
    params = new HashSet<>();
    while (keyMatcher.find()) {
      params.add(keyMatcher.group(1));
    }
  }

  public Set<String> getParams()
  {
    return params;
  }

  @Override
  public URI apply(Event event) throws URISyntaxException
  {
    Map<String, Object> eventMap = event.toMap();
    String processedUri = uriPattern;
    for (String key : params) {
      Object paramValue = eventMap.get(key);
      if (paramValue == null) {
        throw new IAE(
            "ParametrizedUriExtractor with pattern %s requires %s to be set in event, but found %s",
            uriPattern,
            key,
            eventMap
        );
      }
      processedUri = StringUtils.replace(processedUri, StringUtils.format("{%s}", key), paramValue.toString());
    }
    return new URI(processedUri);
  }

  @Override
  public String toString()
  {
    return "ParametrizedUriExtractor{" +
           "uriPattern='" + uriPattern + '\'' +
           ", params=" + params +
           '}';
  }
}
