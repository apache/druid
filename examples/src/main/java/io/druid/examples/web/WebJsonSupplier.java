/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.examples.web;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
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
    return new BufferedReader(new InputStreamReader(url.openStream()));
  }
}
