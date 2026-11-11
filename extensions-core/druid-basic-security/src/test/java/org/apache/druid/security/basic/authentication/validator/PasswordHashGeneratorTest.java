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

import com.google.common.cache.CacheStats;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

public class PasswordHashGeneratorTest
{

  @Test
  public void testHashPassword()
  {
    char[] password = "HELLO".toCharArray();
    int iterations = BasicAuthUtils.DEFAULT_KEY_ITERATIONS;
    byte[] salt = BasicAuthUtils.generateSalt();
    byte[] hash = PasswordHashGenerator.computePasswordHash(password, salt, iterations);

    Assertions.assertEquals(BasicAuthUtils.SALT_LENGTH, salt.length);
    Assertions.assertEquals(PasswordHashGenerator.KEY_LENGTH / 8, hash.length);
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testHashIsNotRecomputedWhenCached()
  {
    final PasswordHashGenerator hashGenerator = new PasswordHashGenerator();

    final char[] password = "this_is_a_long_password".toCharArray();
    final int iterations = BasicAuthUtils.DEFAULT_KEY_ITERATIONS;
    final byte[] salt = BasicAuthUtils.generateSalt();

    final byte[] expectedHash = PasswordHashGenerator.computePasswordHash(password, salt, iterations);

    // Verify that the first computation takes a few ms
    byte[] firstHash = hashGenerator.getOrComputePasswordHash(password, salt, iterations);
    Assertions.assertArrayEquals(expectedHash, firstHash);
    CacheStats stats = hashGenerator.getCacheStats();
    Assertions.assertEquals(0, stats.hitCount());
    Assertions.assertEquals(1, stats.missCount());

    // Verify that each subsequent computation takes less than 1ms
    for (int i = 0; i < 10; ++i) {
      byte[] recomputedHash = hashGenerator.getOrComputePasswordHash(password, salt, iterations);
      Assertions.assertArrayEquals(expectedHash, recomputedHash);
      stats = hashGenerator.getCacheStats();
      Assertions.assertEquals(i + 1, stats.hitCount());
      Assertions.assertEquals(1, stats.missCount());
    }
  }

}
