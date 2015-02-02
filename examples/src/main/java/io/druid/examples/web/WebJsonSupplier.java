/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.examples.web;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.InputSupplier;
import com.metamx.emitter.EmittingLogger;
import org.apache.commons.validator.routines.UrlValidator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class WebJsonSupplier implements InputSupplier<BufferedReader>
{
  private static final EmittingLogger log = new EmittingLogger(WebJsonSupplier.class);
  private static final UrlValidator urlValidator = new UrlValidator();

  private URL url;

  public WebJsonSupplier(String urlString)
  {
    Preconditions.checkState(urlValidator.isValid(urlString));

    try {
      this.url = new URL(urlString);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public BufferedReader getInput() throws IOException
  {
    URLConnection connection = url.openConnection();
    connection.setDoInput(true);
    return new BufferedReader(new InputStreamReader(url.openStream(), Charsets.UTF_8));
  }
}
