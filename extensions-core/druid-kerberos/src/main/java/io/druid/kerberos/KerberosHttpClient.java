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

package io.druid.kerberos;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.http.client.AbstractHttpClient;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.HttpResponseHandler;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.joda.time.Duration;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class KerberosHttpClient extends AbstractHttpClient
{
  private static final Logger log = new Logger(KerberosHttpClient.class);

  private final HttpClient delegate;
  private final DruidKerberosConfig config;
  private static final Base64 base64codec = new Base64(0);


  public KerberosHttpClient(HttpClient delegate, DruidKerberosConfig config)
  {
    this.delegate = delegate;
    this.config = config;
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
    Request request, HttpResponseHandler<Intermediate, Final> httpResponseHandler, Duration duration
  )
  {
    try {
      final String host = request.getUrl().getHost();
      authenticateIfRequired();
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      String challenge = currentUser.doAs(new PrivilegedExceptionAction<String>()
      {
        @Override
        public String run() throws Exception
        {
          return kerberosChallenge(host);
        }
      });
      request.setHeader(HttpHeaders.Names.AUTHORIZATION, "Negotiate " + challenge);
    }
    catch (Throwable e) {
      Throwables.propagate(e);
    }
    return delegate.go(request, httpResponseHandler, duration);
  }


  public void authenticateIfRequired()
    throws IOException
  {
    String principal = config.getPrincipal();
    String keytab = config.getKeytab();
    if (!Strings.isNullOrEmpty(principal) && !Strings.isNullOrEmpty(keytab)) {
      Configuration conf = new Configuration();
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      UserGroupInformation.setConfiguration(conf);
      if (UserGroupInformation.isSecurityEnabled()) {
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
  }

  /**
   * This method always needs to be called within a doAs block so that the client's TGT credentials
   * can be read from the Subject.
   *
   * @return Kerberos Challenge String
   *
   * @throws Exception
   */

  public static String kerberosChallenge(String server) throws GSSException
  {
    // This Oid for Kerberos GSS-API mechanism.
    Oid mechOid = new Oid("1.2.840.113554.1.2.2");
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
    return new String(base64codec.encode(outToken));
  }

}
