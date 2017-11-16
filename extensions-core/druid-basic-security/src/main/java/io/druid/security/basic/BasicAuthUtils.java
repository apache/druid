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

package io.druid.security.basic;

import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.servlet.http.HttpServletRequest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

public class BasicAuthUtils
{
  private static final Logger log = new Logger(BasicAuthUtils.class);
  private static final Base64.Encoder ENCODER = Base64.getEncoder();
  private static final Base64.Decoder DECODER = Base64.getDecoder();
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  public final static String ADMIN_NAME = "admin";
  public final static String INTERNAL_USER_NAME = "druid_system";

  public static int SALT_LENGTH = 32;
  public static int KEY_ITERATIONS = 10000;
  public static int KEY_LENGTH = 512;
  public static String ALGORITHM = "PBKDF2WithHmacSHA512";

  public static String getEncodedCredentials(final String unencodedCreds)
  {
    return ENCODER.encodeToString(StringUtils.toUtf8(unencodedCreds));
  }

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
      throw new RuntimeException(ikse);
    }
    catch (NoSuchAlgorithmException nsae) {
      log.error("%s not supported on this system.", ALGORITHM);
      throw new RuntimeException(nsae);
    }
  }

  public static byte[] generateSalt()
  {
    byte salt[] = new byte[SALT_LENGTH];
    SECURE_RANDOM.nextBytes(salt);
    return salt;
  }

  public static String getBasicUserSecretFromHttpReq(HttpServletRequest httpReq)
  {
    try {
      String authHeader = httpReq.getHeader("Authorization");

      if (authHeader == null) {
        return null;
      }

      if (!authHeader.substring(0, 6).equals("Basic ")) {
        return null;
      }

      String encodedUserSecret = authHeader.substring(6);
      return StringUtils.fromUtf8(DECODER.decode(encodedUserSecret));
    }
    catch (Exception e) {
      return null;
    }
  }
}
