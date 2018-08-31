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

import org.apache.druid.java.util.common.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;

public class FeedUriExtractor implements UriExtractor
{
  private String uriPattern;

  public FeedUriExtractor(String uriPattern)
  {
    this.uriPattern = uriPattern;
  }

  @Override
  public URI apply(Event event) throws URISyntaxException
  {
    return new URI(StringUtils.format(uriPattern, event.getFeed()));
  }

  @Override
  public String toString()
  {
    return "FeedUriExtractor{" +
           "uriPattern='" + uriPattern + '\'' +
           '}';
  }
}
