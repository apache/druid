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

package org.apache.druid.security.basic;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorUser;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import org.apache.druid.security.basic.authorization.entity.UserAndRoleMap;

import javax.annotation.Nullable;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.Map;

public class BasicAuthUtils
{

  private static final Logger log = new Logger(BasicAuthUtils.class);
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  public static final String ADMIN_NAME = "admin";
  public static final String INTERNAL_USER_NAME = "druid_system";

  // PBKDF2WithHmacSHA512 is chosen since it has built-in support in Java8.
  // Argon2 (https://github.com/p-h-c/phc-winner-argon2) is newer but the only presently
  // available Java binding is LGPLv3 licensed.
  // Key length is 512-bit to match the PBKDF2WithHmacSHA512 algorithm.
  // 256-bit salt should be more than sufficient for uniqueness, expected user count is on the order of thousands.
  public static final int SALT_LENGTH = 32;
  public static final int DEFAULT_KEY_ITERATIONS = 10000;
  public static final int KEY_LENGTH = 512;
  public static final String ALGORITHM = "PBKDF2WithHmacSHA512";

  public static final TypeReference AUTHENTICATOR_USER_MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, BasicAuthenticatorUser>>()
      {
      };

  public static final TypeReference AUTHORIZER_USER_MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, BasicAuthorizerUser>>()
      {
      };

  public static final TypeReference AUTHORIZER_ROLE_MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, BasicAuthorizerRole>>()
      {
      };

  public static final TypeReference AUTHORIZER_USER_AND_ROLE_MAP_TYPE_REFERENCE =
      new TypeReference<UserAndRoleMap>()
      {
      };

  public static byte[] hashPassword(final char[] password, final byte[] salt, final int iterations)
  {
    try {
      SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(ALGORITHM);
      SecretKey key = keyFactory.generateSecret(
          new PBEKeySpec(
              password,
              salt,
              iterations,
              KEY_LENGTH
          )
      );
      return key.getEncoded();
    }
    catch (InvalidKeySpecException ikse) {
      log.error("WTF? invalid keyspec");
      throw new RuntimeException("WTF? invalid keyspec", ikse);
    }
    catch (NoSuchAlgorithmException nsae) {
      log.error("%s not supported on this system.", ALGORITHM);
      throw new RE(nsae, "%s not supported on this system.", ALGORITHM);
    }
  }

  public static byte[] generateSalt()
  {
    byte salt[] = new byte[SALT_LENGTH];
    SECURE_RANDOM.nextBytes(salt);
    return salt;
  }

  @Nullable
  public static String getEncodedUserSecretFromHttpReq(HttpServletRequest httpReq)
  {
    String authHeader = httpReq.getHeader("Authorization");

    if (authHeader == null) {
      return null;
    }

    if (authHeader.length() < 7) {
      return null;
    }

    if (!"Basic ".equals(authHeader.substring(0, 6))) {
      return null;
    }

    return authHeader.substring(6);
  }

  @Nullable
  public static String decodeUserSecret(String encodedUserSecret)
  {
    try {
      return StringUtils.fromUtf8(StringUtils.decodeBase64String(encodedUserSecret));
    }
    catch (IllegalArgumentException iae) {
      return null;
    }
  }

  public static Map<String, BasicAuthenticatorUser> deserializeAuthenticatorUserMap(
      ObjectMapper objectMapper,
      byte[] userMapBytes
  )
  {
    Map<String, BasicAuthenticatorUser> userMap;
    if (userMapBytes == null) {
      userMap = new HashMap<>();
    } else {
      try {
        userMap = objectMapper.readValue(userMapBytes, AUTHENTICATOR_USER_MAP_TYPE_REFERENCE);
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return userMap;
  }

  public static byte[] serializeAuthenticatorUserMap(
      ObjectMapper objectMapper,
      Map<String, BasicAuthenticatorUser> userMap
  )
  {
    try {
      return objectMapper.writeValueAsBytes(userMap);
    }
    catch (IOException ioe) {
      throw new ISE(ioe, "WTF? Couldn't serialize userMap!");
    }
  }

  public static Map<String, BasicAuthorizerUser> deserializeAuthorizerUserMap(
      ObjectMapper objectMapper,
      byte[] userMapBytes
  )
  {
    Map<String, BasicAuthorizerUser> userMap;
    if (userMapBytes == null) {
      userMap = new HashMap<>();
    } else {
      try {
        userMap = objectMapper.readValue(userMapBytes, BasicAuthUtils.AUTHORIZER_USER_MAP_TYPE_REFERENCE);
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return userMap;
  }

  public static byte[] serializeAuthorizerUserMap(ObjectMapper objectMapper, Map<String, BasicAuthorizerUser> userMap)
  {
    try {
      return objectMapper.writeValueAsBytes(userMap);
    }
    catch (IOException ioe) {
      throw new ISE(ioe, "WTF? Couldn't serialize userMap!");
    }
  }

  public static Map<String, BasicAuthorizerRole> deserializeAuthorizerRoleMap(
      ObjectMapper objectMapper,
      byte[] roleMapBytes
  )
  {
    Map<String, BasicAuthorizerRole> roleMap;
    if (roleMapBytes == null) {
      roleMap = new HashMap<>();
    } else {
      try {
        roleMap = objectMapper.readValue(roleMapBytes, BasicAuthUtils.AUTHORIZER_ROLE_MAP_TYPE_REFERENCE);
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return roleMap;
  }

  public static byte[] serializeAuthorizerRoleMap(ObjectMapper objectMapper, Map<String, BasicAuthorizerRole> roleMap)
  {
    try {
      return objectMapper.writeValueAsBytes(roleMap);
    }
    catch (IOException ioe) {
      throw new ISE(ioe, "WTF? Couldn't serialize roleMap!");
    }
  }
}
