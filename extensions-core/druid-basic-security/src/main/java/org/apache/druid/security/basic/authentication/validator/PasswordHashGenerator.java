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

package org.apache.druid.security.basic.authentication.validator;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.hash.Hashing;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.basic.BasicAuthUtils;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Generates a hash of passwords using the {@link #HASH_ALGORITHM}. Multiple
 * iterations may be used to enhance security of the generated hash.
 * <p>
 * Hashes once computed are cached for an hour so that they need not be recomputed
 * for every API invocation.
 */
public class PasswordHashGenerator
{
  private static final Logger log = new Logger(PasswordHashGenerator.class);

  public static final int KEY_LENGTH = 512;
  public static final String HASH_ALGORITHM = "PBKDF2WithHmacSHA512";

  /**
   * Salt used to compute a quick sha-256 hash of the password for the cache.
   */
  private final byte[] shaSalt = BasicAuthUtils.generateSalt();

  private final Cache<CacheKey, byte[]> cache = CacheBuilder.newBuilder()
                                                            .maximumSize(1000)
                                                            .recordStats()
                                                            .expireAfterAccess(Duration.ofMinutes(60))
                                                            .build();

  /**
   * Hashes the given password using the {@link #HASH_ALGORITHM}.
   */
  public byte[] getOrComputePasswordHash(char[] password, byte[] salt, int numIterations)
  {
    try {
      return cache.get(
          CacheKey.of(password, salt, numIterations, shaSalt),
          () -> computePasswordHash(password, salt, numIterations)
      );
    }
    catch (ExecutionException e) {
      throw DruidException.defensive().build(e, "Could not compute hash of password");
    }
  }

  public CacheStats getCacheStats()
  {
    return cache.stats();
  }

  /**
   * Utility method to compuate hash of the given password using the {@link #HASH_ALGORITHM}.
   * Callers should use the non-static method instead to leverage caching.
   */
  public static byte[] computePasswordHash(final char[] password, final byte[] salt, final int iterations)
  {
    try {
      SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(HASH_ALGORITHM);
      SecretKey key = keyFactory.generateSecret(new PBEKeySpec(password, salt, iterations, KEY_LENGTH));
      return key.getEncoded();
    }
    catch (InvalidKeySpecException ikse) {
      log.error("Invalid keyspec");
      throw new RuntimeException("Invalid keyspec", ikse);
    }
    catch (NoSuchAlgorithmException nsae) {
      log.error("Hash algorithm[%s] is not supported on this system.", HASH_ALGORITHM);
      throw new RE(nsae, "Hash algorithm[%s] is not supported on this system.", HASH_ALGORITHM);
    }
  }

  /**
   * Key used in the {@link #cache}. An SHA-256 hash of the password is used
   * instead of the actual string so that the passwords are never exposed, even
   * in a heap dump.
   */
  private static class CacheKey
  {
    final byte[] passwordSha;
    final byte[] salt;
    final int numIterations;

    CacheKey(byte[] passwordSha, byte[] salt, int numIterations)
    {
      this.passwordSha = passwordSha;
      this.salt = salt;
      this.numIterations = numIterations;
    }

    static CacheKey of(char[] password, byte[] salt, int numIterations, byte[] md5Salt)
    {
      @SuppressWarnings("UnstableApiUsage")
      byte[] passwordSha = Hashing.sha256().newHasher()
                                  .putBytes(StringUtils.toUtf8(new String(password)))
                                  .putBytes(md5Salt)
                                  .hash()
                                  .asBytes();

      return new CacheKey(passwordSha, salt, numIterations);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return numIterations == cacheKey.numIterations
             && Arrays.equals(passwordSha, cacheKey.passwordSha)
             && Arrays.equals(salt, cacheKey.salt);
    }

    @Override
    public int hashCode()
    {
      int result = Objects.hash(numIterations);
      result = 31 * result + Arrays.hashCode(passwordSha);
      result = 31 * result + Arrays.hashCode(salt);
      return result;
    }
  }
}
