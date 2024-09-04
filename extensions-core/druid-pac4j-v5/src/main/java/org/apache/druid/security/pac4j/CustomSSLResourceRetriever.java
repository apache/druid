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

package org.apache.druid.security.pac4j;


import com.google.common.primitives.Ints;
import com.nimbusds.jose.util.DefaultResourceRetriever;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 * This class exists only to enable use of custom SSLSocketFactory on top of builtin class. This could be removed
 * when same functionality has been added to original class com.nimbusds.jose.util.DefaultResourceRetriever.
 */
public class CustomSSLResourceRetriever extends DefaultResourceRetriever
{
  private SSLSocketFactory sslSocketFactory;

  public CustomSSLResourceRetriever(long readTimeout, SSLSocketFactory sslSocketFactory)
  {
    // super(..) has to be the very first statement in constructor.
    super(Ints.checkedCast(readTimeout), Ints.checkedCast(readTimeout));

    this.sslSocketFactory = sslSocketFactory;
  }

  @Override
  protected HttpURLConnection openConnection(final URL url) throws IOException
  {
    HttpURLConnection con = super.openConnection(url);

    if (sslSocketFactory != null && con instanceof HttpsURLConnection) {
      ((HttpsURLConnection) con).setSSLSocketFactory(sslSocketFactory);
    }

    return con;
  }
}
