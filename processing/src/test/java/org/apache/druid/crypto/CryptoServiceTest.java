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

import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public class CryptoServiceTest
{
  private static final String PASSPHRASE = "random-passphrase";
  private static final CryptoService CRYPTO_SERVICE = createCryptoService(PASSPHRASE);

  /**
   * Fixture written by the legacy AES/CBC/PKCS5Padding implementation with an eight-byte salt and a 16-byte IV.
   */
  private static final byte[] LEGACY_CIPHERTEXT = Base64.getDecoder().decode(
      "AAAACAAAABAAAAAgAAECAwQFBgcQERITFBUWFxgZGhscHR4fv7hZzZdQZX4Q18kH0DXtzmraN52ciCGxrcutYbQ3qp8="
  );

  @Test
  public void testEncryptDecrypt()
  {
    final byte[] original = "i am a test string".getBytes(StandardCharsets.UTF_8);

    final byte[] encrypted = CRYPTO_SERVICE.encrypt(original);
    final byte[] decrypted = CRYPTO_SERVICE.decrypt(encrypted);

    Assertions.assertArrayEquals(original, decrypted);
  }

  @Test
  public void testEncryptUsesFreshAuthenticatedCiphertext()
  {
    final byte[] original = "i am a test string".getBytes(StandardCharsets.UTF_8);
    final byte[] first = CRYPTO_SERVICE.encrypt(original);
    final byte[] second = CRYPTO_SERVICE.encrypt(original);

    Assertions.assertFalse(Arrays.equals(first, second));
    Assertions.assertArrayEquals(original, CRYPTO_SERVICE.decrypt(first));
    Assertions.assertArrayEquals(original, CRYPTO_SERVICE.decrypt(second));
  }

  @Test
  public void testDecryptLegacyCiphertext()
  {
    Assertions.assertEquals(
        "legacy ciphertext",
        new String(CRYPTO_SERVICE.decrypt(LEGACY_CIPHERTEXT), StandardCharsets.UTF_8)
    );
  }

  @Test
  public void testAuthenticatedCiphertextRejectsTampering()
  {
    final byte[] encrypted = CRYPTO_SERVICE.encrypt("authenticated".getBytes(StandardCharsets.UTF_8));

    // Format version, salt, IV, and cipher text are all protected or safely rejected during parsing.
    assertDecryptionFails(withFlippedByte(encrypted, 4));
    assertDecryptionFails(withFlippedByte(encrypted, 17));
    assertDecryptionFails(withFlippedByte(encrypted, 25));
    assertDecryptionFails(withFlippedByte(encrypted, encrypted.length - 1));
    assertDecryptionFails(Arrays.copyOf(encrypted, encrypted.length - 1));
  }

  @Test
  public void testAuthenticatedCiphertextCannotBeDowngradedToLegacyFormat()
  {
    final byte[] encrypted = CRYPTO_SERVICE.encrypt("authenticated".getBytes(StandardCharsets.UTF_8));
    assertDecryptionFails(withFlippedByte(encrypted, 0));
  }

  @Test
  public void testAuthenticatedCiphertextRejectsWrongPassphrase()
  {
    final byte[] encrypted = CRYPTO_SERVICE.encrypt("authenticated".getBytes(StandardCharsets.UTF_8));
    final CryptoService otherCryptoService = createCryptoService("different-passphrase");

    Assertions.assertThrows(DruidException.class, () -> otherCryptoService.decrypt(encrypted));
  }

  @Test
  public void testMalformedLegacyLengthsAreRejected()
  {
    final byte[] malformed = ByteBuffer.allocate(12)
                                       .putInt(Integer.MAX_VALUE)
                                       .putInt(Integer.MAX_VALUE)
                                       .putInt(Integer.MAX_VALUE)
                                       .array();
    assertDecryptionFails(malformed);
  }

  @Test
  public void testInvalidParamsConstructorFailure()
  {
    Assertions.assertThrows(
        RuntimeException.class,
        () -> new CryptoService(
            PASSPHRASE,
            "ABCD",
            "EFGH",
            "PAXXDDING",
            "QWERTY",
            8,
            65536,
            128
        )
    );
  }

  @Test
  public void testInvalidLegacyCipherParametersFailWhenDecryptingLegacyCiphertext()
  {
    final CryptoService cryptoService = new CryptoService(
        PASSPHRASE,
        "ABCD",
        "EFGH",
        "PAXXDDING",
        "PBKDF2WithHmacSHA256",
        8,
        65536,
        128
    );

    Assertions.assertThrows(DruidException.class, () -> cryptoService.decrypt(LEGACY_CIPHERTEXT));
  }

  private static CryptoService createCryptoService(final String passphrase)
  {
    return new CryptoService(
        passphrase,
        "AES",
        "CBC",
        "PKCS5Padding",
        "PBKDF2WithHmacSHA256",
        8,
        65536,
        128
    );
  }

  private static byte[] withFlippedByte(final byte[] original, final int index)
  {
    final byte[] tampered = original.clone();
    tampered[index] ^= 0x01;
    return tampered;
  }

  private static void assertDecryptionFails(final byte[] encrypted)
  {
    Assertions.assertThrows(DruidException.class, () -> CRYPTO_SERVICE.decrypt(encrypted));
  }
}
