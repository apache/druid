/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.segment.loading;

import com.google.common.base.Predicate;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * A URIDataPuller has handlings for URI based data
 */
public interface URIDataPuller
{
  /**
   * Create a new InputStream based on the URI
   *
   * @param uri The URI to open an Input Stream to
   *
   * @return A new InputStream which streams the URI in question
   *
   * @throws IOException
   */
  public InputStream getInputStream(URI uri) throws IOException;

  /**
   * Returns an abstract "version" for the URI. The exact meaning of the version is left up to the implementation.
   *
   * @param uri The URI to check
   *
   * @return A "version" as interpreted by the URIDataPuller implementation
   *
   * @throws IOException on error
   */
  public String getVersion(URI uri) throws IOException;

  /**
   * Evaluates a Throwable to see if it is recoverable. This is expected to be used in conjunction with the other methods
   * to determine if anything thrown from the method should be retried.
   *
   * @return Predicate function indicating if the Throwable is recoverable
   */
  public Predicate<Throwable> shouldRetryPredicate();
}
