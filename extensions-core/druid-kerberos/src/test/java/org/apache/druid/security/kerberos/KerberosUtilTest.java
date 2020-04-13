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

package org.apache.druid.security.kerberos;

import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.junit.Assert;
import org.junit.Test;

import java.net.CookieManager;
import java.net.CookieStore;
import java.net.HttpCookie;
import java.net.URI;
import java.net.URISyntaxException;

public class KerberosUtilTest
{

  @Test
  public void testDruidUtil() throws URISyntaxException
  {
    CookieManager manager = new CookieManager();
    CookieStore cookieStore = manager.getCookieStore();
    HttpCookie cookie1 = new HttpCookie(AuthenticatedURL.AUTH_COOKIE, "cookie1");
    cookie1.setSecure(true);
    cookieStore.add(new URI("http://test1.druid.apache.com/abc/def"), cookie1);

    // mismatch domain name
    Assert.assertNull(DruidKerberosUtil.getAuthCookie(cookieStore, new URI("https://test2.druid.apache.com/def")));

    // cookie is secure and the url is unsecure
    Assert.assertNull(DruidKerberosUtil.getAuthCookie(cookieStore, new URI("http://test1.druid.apache.com/def")));

    Assert.assertEquals(cookie1, DruidKerberosUtil.getAuthCookie(cookieStore, new URI("https://test1.druid.apache.com/def")));


  }
}
