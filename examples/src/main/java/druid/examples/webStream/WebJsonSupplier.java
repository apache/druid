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

package druid.examples.webStream;

import com.google.common.io.InputSupplier;
import com.metamx.emitter.EmittingLogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

public class WebJsonSupplier implements InputSupplier<BufferedReader>
{
  private static final EmittingLogger log = new EmittingLogger(WebJsonSupplier.class);

  private String urlString;
  private URL url;

  public WebJsonSupplier(String urlString)
  {
    this.urlString = urlString;
    try {
      this.url = new URL(urlString);
    }
    catch (Exception e) {
      log.error(e,"Malformed url");
    }
  }

  @Override
  public BufferedReader getInput() throws IOException
  {
    URL url = new URL(urlString);
    URLConnection connection = url.openConnection();
    connection.setDoInput(true);
    return new BufferedReader(new InputStreamReader(url.openStream()));
  }
}
