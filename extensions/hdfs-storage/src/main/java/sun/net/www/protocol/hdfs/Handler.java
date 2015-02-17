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

package sun.net.www.protocol.hdfs;

import com.metamx.common.IAE;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 *
 */
public class Handler extends URLStreamHandler
{
  private static Configuration conf = new Configuration(true);

  public static void setConfiguration(Configuration conf){
    Handler.conf = conf;
  }

  @Override
  protected URLConnection openConnection(final URL u) throws IOException
  {
    final org.apache.hadoop.fs.Path path;
    try {
      path = new org.apache.hadoop.fs.Path(u.toURI());
    }
    catch (URISyntaxException e) {
      throw new IAE(e, "Malformed URL [%s]", u.toString());
    }

    final URLConnection connection = new URLConnection(u)
    {
      private final org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(new Configuration(true));

      @Override
      public void connect() throws IOException
      {
        if (!fs.exists(path)) {
          throw new IOException("File does not exist");
        }
      }

      @Override
      public InputStream getInputStream() throws IOException
      {
        return fs.open(path);
      }
    };
    connection.connect();
    return connection;
  }
}
