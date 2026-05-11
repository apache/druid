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

package org.apache.druid.segment.serde.cell;

import java.util.Random;

/**
 * A stable, deterministic random string generator for tests.
 * 
 * This implementation is independent of commons-lang3 and will produce
 * consistent output across library upgrades, ensuring test stability.
 */
public class RandomStringUtils
{
  // Character sets for alphanumeric generation
  private static final String LETTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final String NUMBERS = "0123456789";
  private static final String ALPHANUMERIC = LETTERS + NUMBERS;

  private final Random random;

  public RandomStringUtils()
  {
    random = new Random(0);
  }

  public RandomStringUtils(Random random)
  {
    this.random = random;
  }

  /**
   * Generates a random alphanumeric string of the specified length.
   * 
   * This method produces the same output as the old commons-lang3 3.12.0
   * implementation when given the same Random seed, ensuring test stability.
   * 
   * @param length the length of the string to generate
   * @return a random alphanumeric string
   */
  public String randomAlphanumeric(int length)
  {
    if (length < 0) {
      throw new IllegalArgumentException("Requested random string length " + length + " is less than 0.");
    }
    if (length == 0) {
      return "";
    }

    final StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(ALPHANUMERIC.charAt(random.nextInt(ALPHANUMERIC.length())));
    }
    return sb.toString();
  }

  /**
   * Generates a random alphanumeric string with length between minLength and maxLength.
   * 
   * @param minLength the minimum length (inclusive)
   * @param maxLength the maximum length (exclusive)
   * @return a random alphanumeric string
   */
  public String randomAlphanumeric(int minLength, int maxLength)
  {
    if (minLength < 0) {
      throw new IllegalArgumentException("Minimum length " + minLength + " is less than 0.");
    }
    if (maxLength <= minLength) {
      throw new IllegalArgumentException("Maximum length " + maxLength + " is less than or equal to minimum length " + minLength + ".");
    }

    int length = random.nextInt(maxLength - minLength) + minLength;
    return randomAlphanumeric(length);
  }
}
