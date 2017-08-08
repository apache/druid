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

package io.druid.security.kerberos;

import com.google.common.base.Strings;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import java.io.IOException;
import java.net.CookieStore;
import java.net.HttpCookie;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class DruidKerberosUtil
{
  private static final Logger log = new Logger(DruidKerberosUtil.class);

  private static final Base64 base64codec = new Base64(0);

  // A fair reentrant lock
  private static ReentrantLock kerberosLock = new ReentrantLock(true);

  /**
   * This method always needs to be called within a doAs block so that the client's TGT credentials
   * can be read from the Subject.
   *
   * @return Kerberos Challenge String
   *
   * @throws Exception
   */

  public static String kerberosChallenge(String server) throws AuthenticationException
  {
    kerberosLock.lock();
    try {
      // This Oid for Kerberos GSS-API mechanism.
      Oid mechOid = KerberosUtil.getOidInstance("GSS_KRB5_MECH_OID");
      GSSManager manager = GSSManager.getInstance();
      // GSS name for server
      GSSName serverName = manager.createName("HTTP@" + server, GSSName.NT_HOSTBASED_SERVICE);
      // Create a GSSContext for authentication with the service.
      // We're passing client credentials as null since we want them to be read from the Subject.
      GSSContext gssContext =
        manager.createContext(serverName.canonicalize(mechOid), mechOid, null, GSSContext.DEFAULT_LIFETIME);
      gssContext.requestMutualAuth(true);
      gssContext.requestCredDeleg(true);
      // Establish context
      byte[] inToken = new byte[0];
      byte[] outToken = gssContext.initSecContext(inToken, 0, inToken.length);
      gssContext.dispose();
      // Base64 encoded and stringified token for server
      return new String(base64codec.encode(outToken), StandardCharsets.US_ASCII);
    }
    catch (GSSException | IllegalAccessException | NoSuchFieldException | ClassNotFoundException e) {
      throw new AuthenticationException(e);
    }
    finally {
      kerberosLock.unlock();
    }
  }

  public static void authenticateIfRequired(AuthenticationKerberosConfig config)
    throws IOException
  {
    String principal = config.getPrincipal();
    String keytab = config.getKeytab();
    if (!Strings.isNullOrEmpty(principal) && !Strings.isNullOrEmpty(keytab)) {
      Configuration conf = new Configuration();
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      UserGroupInformation.setConfiguration(conf);
      try {
        if (UserGroupInformation.getCurrentUser().hasKerberosCredentials() == false
            || !UserGroupInformation.getCurrentUser().getUserName().equals(principal)) {
          log.info("trying to authenticate user [%s] with keytab [%s]", principal, keytab);
          UserGroupInformation.loginUserFromKeytab(principal, keytab);
        }
      }
      catch (IOException e) {
        throw new ISE(e, "Failed to authenticate user principal [%s] with keytab [%s]", principal, keytab);
      }
    }
  }

  public static boolean needToSendCredentials(CookieStore cookieStore, URI uri)
  {
    return getAuthCookie(cookieStore, uri) == null;
  }

  public static HttpCookie getAuthCookie(CookieStore cookieStore, URI uri)
  {
    if (cookieStore == null) {
      return null;
    }
    boolean isSSL = uri.getScheme().equals("https");
    List<HttpCookie> cookies = cookieStore.getCookies();

    for (HttpCookie c : cookies) {
      // If this is a secured cookie and the current connection is non-secured,
      // then, skip this cookie. We need to skip this cookie because, the cookie
      // replay will not be transmitted to the server.
      if (c.getSecure() && !isSSL) {
        continue;
      }
      if (c.getName().equals(AuthenticatedURL.AUTH_COOKIE)) {
        return c;
      }
    }
    return null;
  }

  public static void removeAuthCookie(CookieStore cookieStore, URI uri)
  {
    HttpCookie authCookie = getAuthCookie(cookieStore, uri);
    if (authCookie != null) {
      cookieStore.remove(uri, authCookie);
    }
  }
}
