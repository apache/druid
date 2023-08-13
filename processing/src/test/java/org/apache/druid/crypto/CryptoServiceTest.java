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

package org.apache.druid.crypto;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class CryptoServiceTest
{
  @Test
  public void testEncryptDecrypt()
  {
    CryptoService cryptoService = new CryptoService(
        "random-passphrase",
        "AES",
        "CBC",
        "PKCS5Padding",
        "PBKDF2WithHmacSHA256",
        8,
        65536,
        128
    );

    byte[] original = "i am a test string".getBytes(StandardCharsets.UTF_8);

    byte[] decrypted = cryptoService.decrypt(cryptoService.encrypt(original));

    Assert.assertArrayEquals(original, decrypted);
  }

  @Test
  public void testInvalidParamsConstructorFailure()
  {
    try {
      new CryptoService(
          "random-passphrase",
          "ABCD",
          "EFGH",
          "PAXXDDING",
          "QWERTY",
          8,
          65536,
          128
      );
      Assert.fail("Must Fail!!!");
    }
    catch (RuntimeException ex) {
      // expected
    }
  }
}
