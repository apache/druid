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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import java.security.spec.KeySpec;

public class CryptoService
{
  private static final int ITERATION_COUNT = 65536;
  private static final int KEY_LENGTH = 128;

  private char[] passPhrase;
  private String alg = "AES";
  private String pbeAlg = "PBKDF2WithHmacSHA1";
  private String transformation = "AES/CBC/PKCS5Padding";
  private int saltSize = 8;
  private int iterationCount = ITERATION_COUNT;
  private int keyLength = KEY_LENGTH;

  public CryptoService(String passPhrase)
  {
    this(passPhrase, null, null, null, null, null, null);
  }

  public CryptoService(
      String passPhrase,
      @Nullable String alg,
      @Nullable String pbeAlg,
      @Nullable String transformation,
      @Nullable Integer saltSize,
      @Nullable Integer iterationCount,
      @Nullable Integer keyLength
  )
  {
    Preconditions.checkArgument(
        passPhrase != null && !passPhrase.isEmpty(),
        "null/empty passPhrase"
    );
    this.passPhrase = passPhrase.toCharArray();

    if (alg != null) {
      this.alg = alg;
    }

    if (pbeAlg != null) {
      this.pbeAlg = pbeAlg;
    }

    if (transformation != null) {
      this.transformation = transformation;
    }

    if (saltSize != null) {
      this.saltSize = saltSize;
    }

    if (iterationCount != null) {
      this.iterationCount = iterationCount;
    }

    if (keyLength != null) {
      this.keyLength = keyLength;
    }

    // encrypt/decrypt a test string to ensure all params are valid
    String testString = "duh! !! !!!";
    Preconditions.checkState(
        testString.equals(StringUtils.fromUtf8(decrypt(encrypt(StringUtils.toUtf8(testString))))),
        "decrypt(encrypt(testString)) failed"
    );
  }

  public byte[] encrypt(byte[] plain)
  {
    try {
      byte[] salt = new byte[saltSize];
      SecureRandom rnd = new SecureRandom();
      rnd.nextBytes(salt);

      SecretKey tmp = getKeyFromPassword(passPhrase, salt);
      SecretKey secret = new SecretKeySpec(tmp.getEncoded(), alg);
      Cipher ecipher = Cipher.getInstance(transformation);
      ecipher.init(Cipher.ENCRYPT_MODE, secret);
      return new EncryptionResult(
          salt,
          ecipher.getParameters().getParameterSpec(IvParameterSpec.class).getIV(),
          ecipher.doFinal(plain)
      ).toByteAray();
    }
    catch (InvalidKeySpecException | NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidParameterSpecException | IllegalBlockSizeException | BadPaddingException ex) {
      throw new RuntimeException(ex);
    }
  }

  public byte[] decrypt(byte[] data)
  {
    try {
      EncryptionResult encryptionResult = EncryptionResult.fromByteArray(data);

      SecretKey tmp = getKeyFromPassword(passPhrase, encryptionResult.getSalt());
      SecretKey secret = new SecretKeySpec(tmp.getEncoded(), alg);

      Cipher dcipher = Cipher.getInstance(transformation);
      dcipher.init(Cipher.DECRYPT_MODE, secret, new IvParameterSpec(encryptionResult.getIv()));
      return dcipher.doFinal(encryptionResult.getCipher());
    }
    catch (InvalidKeySpecException | NoSuchAlgorithmException | InvalidAlgorithmParameterException | NoSuchPaddingException | InvalidKeyException | IllegalBlockSizeException | BadPaddingException ex) {
      throw new RuntimeException(ex);
    }
  }

  private SecretKey getKeyFromPassword(char[] passPhrase, byte[] salt)
      throws NoSuchAlgorithmException, InvalidKeySpecException
  {
    SecretKeyFactory factory = SecretKeyFactory.getInstance(pbeAlg);
    KeySpec spec = new PBEKeySpec(passPhrase, salt, iterationCount, keyLength);
    return factory.generateSecret(spec);
  }
}
