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
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import java.security.spec.KeySpec;

/**
 * Utility class for symmetric key encryption (i.e. same secret is used for encryption and decryption) of byte[]
 * using javax.crypto package.
 *
 * To learn about possible algorithms supported and their names,
 * See https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html
 */
public class CryptoService
{
  // Based on Javadocs on SecureRandom, It is threadsafe as well.
  private static final SecureRandom SECURE_RANDOM_INSTANCE = new SecureRandom();

  // User provided secret phrase used for encrypting data
  private final char[] passPhrase;

  // Variables for algorithm used to generate a SecretKey based on user provided passPhrase
  private final String secretKeyFactoryAlg;
  private final int saltSize;
  private final int iterationCount;
  private final int keyLength;

  // Cipher algorithm information
  private final String cipherAlgName;
  private final String cipherAlgMode;
  private final String cipherAlgPadding;

  // transformation =  "cipherAlgName/cipherAlgMode/cipherAlgPadding" used in Cipher.getInstance(transformation)
  private final String transformation;

  public CryptoService(
      String passPhrase,
      @Nullable String cipherAlgName,
      @Nullable String cipherAlgMode,
      @Nullable String cipherAlgPadding,
      @Nullable String secretKeyFactoryAlg,
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

    this.cipherAlgName = cipherAlgName == null ? "AES" : cipherAlgName;
    this.cipherAlgMode = cipherAlgMode == null ? "CBC" : cipherAlgMode;
    this.cipherAlgPadding = cipherAlgPadding == null ? "PKCS5Padding" : cipherAlgPadding;
    this.transformation = StringUtils.format("%s/%s/%s", this.cipherAlgName, this.cipherAlgMode, this.cipherAlgPadding);

    this.secretKeyFactoryAlg = secretKeyFactoryAlg == null ? "PBKDF2WithHmacSHA256" : secretKeyFactoryAlg;
    this.saltSize = saltSize == null ? 8 : saltSize;
    this.iterationCount = iterationCount == null ? 65536 : iterationCount;
    this.keyLength = keyLength == null ? 128 : keyLength;

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
      SECURE_RANDOM_INSTANCE.nextBytes(salt);

      SecretKey tmp = getKeyFromPassword(passPhrase, salt);
      SecretKey secret = new SecretKeySpec(tmp.getEncoded(), cipherAlgName);
      Cipher ecipher = Cipher.getInstance(transformation);
      ecipher.init(Cipher.ENCRYPT_MODE, secret);
      return new EncryptedData(
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
      EncryptedData encryptedData = EncryptedData.fromByteArray(data);

      SecretKey tmp = getKeyFromPassword(passPhrase, encryptedData.getSalt());
      SecretKey secret = new SecretKeySpec(tmp.getEncoded(), cipherAlgName);

      Cipher dcipher = Cipher.getInstance(transformation);
      dcipher.init(Cipher.DECRYPT_MODE, secret, new IvParameterSpec(encryptedData.getIv()));
      return dcipher.doFinal(encryptedData.getCipher());
    }
    catch (InvalidKeySpecException | NoSuchAlgorithmException | InvalidAlgorithmParameterException | NoSuchPaddingException | InvalidKeyException | IllegalBlockSizeException | BadPaddingException ex) {
      throw new RuntimeException(ex);
    }
  }

  private SecretKey getKeyFromPassword(char[] passPhrase, byte[] salt)
      throws NoSuchAlgorithmException, InvalidKeySpecException
  {
    SecretKeyFactory factory = SecretKeyFactory.getInstance(secretKeyFactoryAlg);
    KeySpec spec = new PBEKeySpec(passPhrase, salt, iterationCount, keyLength);
    return factory.generateSecret(spec);
  }

  private static class EncryptedData
  {
    private final byte[] salt;
    private final byte[] iv;
    private final byte[] cipher;

    public EncryptedData(byte[] salt, byte[] iv, byte[] cipher)
    {
      this.salt = salt;
      this.iv = iv;
      this.cipher = cipher;
    }

    public byte[] getSalt()
    {
      return salt;
    }

    public byte[] getIv()
    {
      return iv;
    }

    public byte[] getCipher()
    {
      return cipher;
    }

    public byte[] toByteAray()
    {
      int headerLength = 12;
      ByteBuffer bb = ByteBuffer.allocate(salt.length + iv.length + cipher.length + headerLength);
      bb.putInt(salt.length)
        .putInt(iv.length)
        .putInt(cipher.length)
        .put(salt)
        .put(iv)
        .put(cipher);
      bb.flip();

      return bb.array();
    }

    public static EncryptedData fromByteArray(byte[] array)
    {
      ByteBuffer bb = ByteBuffer.wrap(array);

      int saltSize = bb.getInt();
      int ivSize = bb.getInt();
      int cipherSize = bb.getInt();

      byte[] salt = new byte[saltSize];
      bb.get(salt);

      byte[] iv = new byte[ivSize];
      bb.get(iv);

      byte[] cipher = new byte[cipherSize];
      bb.get(cipher);

      return new EncryptedData(salt, iv, cipher);
    }
  }
}
