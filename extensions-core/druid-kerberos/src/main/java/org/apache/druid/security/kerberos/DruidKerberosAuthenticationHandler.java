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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class DruidKerberosAuthenticationHandler extends KerberosAuthenticationHandler
{
  private static final Logger log = new Logger(DruidKerberosAuthenticationHandler.class);

  private String keytab;
  private GSSManager gssManager;
  private Subject serverSubject = new Subject();
  private List<LoginContext> loginContexts = new ArrayList<LoginContext>();

  @Override
  public void destroy()
  {
    keytab = null;
    serverSubject = null;
    for (LoginContext loginContext : loginContexts) {
      try {
        loginContext.logout();
      }
      catch (LoginException ex) {
        log.warn(ex, ex.getMessage());
      }
    }
    loginContexts.clear();
  }

  @Override
  public void init(Properties config) throws ServletException
  {
    try {
      String principal = config.getProperty(PRINCIPAL);
      if (principal == null || principal.trim().length() == 0) {
        throw new ServletException("Principal not defined in configuration");
      }
      keytab = config.getProperty(KEYTAB, keytab);
      if (keytab == null || keytab.trim().length() == 0) {
        throw new ServletException("Keytab not defined in configuration");
      }
      if (!new File(keytab).exists()) {
        throw new ServletException("Keytab does not exist: " + keytab);
      }

      // use all SPNEGO principals in the keytab if a principal isn't
      // specifically configured
      final String[] spnegoPrincipals;
      if ("*".equals(principal)) {
        spnegoPrincipals = KerberosUtil.getPrincipalNames(keytab, Pattern.compile("HTTP/.*"));
        if (spnegoPrincipals.length == 0) {
          throw new ServletException("Principals do not exist in the keytab");
        }
      } else {
        spnegoPrincipals = new String[]{principal};
      }

      String nameRules = config.getProperty(NAME_RULES, null);
      if (nameRules != null) {
        KerberosName.setRules(nameRules);
      }

      for (String spnegoPrincipal : spnegoPrincipals) {
        log.info("Login using keytab %s, for principal %s", keytab, spnegoPrincipal);
        final KerberosAuthenticator.DruidKerberosConfiguration kerberosConfiguration =
            new KerberosAuthenticator.DruidKerberosConfiguration(keytab, spnegoPrincipal);
        final LoginContext loginContext =
            new LoginContext("", serverSubject, null, kerberosConfiguration);
        try {
          loginContext.login();
        }
        catch (LoginException le) {
          log.warn(le, "Failed to login as [%s]", spnegoPrincipal);
          throw new AuthenticationException(le);
        }
        loginContexts.add(loginContext);
      }
      try {
        gssManager = Subject.doAs(serverSubject, new PrivilegedExceptionAction<GSSManager>()
        {

          @Override
          public GSSManager run()
          {
            return GSSManager.getInstance();
          }
        });
      }
      catch (PrivilegedActionException ex) {
        throw ex.getException();
      }
    }
    catch (Exception ex) {
      throw new ServletException(ex);
    }
  }

  @Override
  public AuthenticationToken authenticate(HttpServletRequest request, final HttpServletResponse response)
      throws IOException, AuthenticationException
  {
    AuthenticationToken token;
    String authorization = request
        .getHeader(org.apache.hadoop.security.authentication.client.KerberosAuthenticator.AUTHORIZATION);

    if (authorization == null ||
        !authorization.startsWith(org.apache.hadoop.security.authentication.client.KerberosAuthenticator.NEGOTIATE)) {
      return null;
    } else {
      authorization = authorization
          .substring(org.apache.hadoop.security.authentication.client.KerberosAuthenticator.NEGOTIATE.length())
          .trim();
      final byte[] clientToken = StringUtils.decodeBase64String(authorization);
      final String serverName = request.getServerName();
      try {
        token = Subject.doAs(serverSubject, new PrivilegedExceptionAction<AuthenticationToken>()
        {

          @Override
          public AuthenticationToken run() throws Exception
          {
            AuthenticationToken token = null;
            GSSContext gssContext = null;
            GSSCredential gssCreds = null;
            try {
              gssCreds = gssManager.createCredential(
                  gssManager.createName(
                      KerberosUtil.getServicePrincipal("HTTP", serverName),
                      KerberosUtil.getOidInstance("NT_GSS_KRB5_PRINCIPAL")
                  ),
                  GSSCredential.INDEFINITE_LIFETIME,
                  new Oid[]{
                      KerberosUtil.getOidInstance("GSS_SPNEGO_MECH_OID"),
                      KerberosUtil.getOidInstance("GSS_KRB5_MECH_OID")
                  },
                  GSSCredential.ACCEPT_ONLY
              );
              gssContext = gssManager.createContext(gssCreds);
              byte[] serverToken = gssContext.acceptSecContext(clientToken, 0, clientToken.length);
              if (serverToken != null && serverToken.length > 0) {
                String authenticate = StringUtils.encodeBase64String(serverToken);
                response.setHeader(
                    org.apache.hadoop.security.authentication.client.KerberosAuthenticator.WWW_AUTHENTICATE,
                    org.apache.hadoop.security.authentication.client.KerberosAuthenticator.NEGOTIATE
                    + " "
                    + authenticate
                );
              }
              if (!gssContext.isEstablished()) {
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                log.trace("SPNEGO in progress");
              } else {
                String clientPrincipal = gssContext.getSrcName().toString();
                KerberosName kerberosName = new KerberosName(clientPrincipal);
                String userName = kerberosName.getShortName();
                token = new AuthenticationToken(userName, clientPrincipal, getType());
                response.setStatus(HttpServletResponse.SC_OK);
                log.trace("SPNEGO completed for principal [%s]", clientPrincipal);
              }
            }
            finally {
              if (gssContext != null) {
                gssContext.dispose();
              }
              if (gssCreds != null) {
                gssCreds.dispose();
              }
            }
            return token;
          }
        });
      }
      catch (PrivilegedActionException ex) {
        if (ex.getException() instanceof IOException) {
          throw (IOException) ex.getException();
        } else {
          throw new AuthenticationException(ex.getException());
        }
      }
    }
    return token;
  }
}
