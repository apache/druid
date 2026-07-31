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
import org.apache.druid.error.InternalServerError;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Arrays;

/**
 * Utility class for symmetric key encryption (i.e. same secret is used for encryption and decryption) of byte[]
 * using javax.crypto package.
 *
 * To learn about possible algorithms supported and their names,
 * See https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html
 */
public class CryptoService
{
  private static final Logger log = new Logger(CryptoService.class);

  // Based on Javadocs on SecureRandom, It is threadsafe as well.
  private static final SecureRandom SECURE_RANDOM_INSTANCE = new SecureRandom();

  private static final String AUTHENTICATED_CIPHER_ALGORITHM = "AES";
  private static final String AUTHENTICATED_CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
  private static final int GCM_IV_SIZE = 12;
  private static final int GCM_TAG_LENGTH_BITS = 128;
  private static final int AUTHENTICATED_FORMAT_MAGIC = 0xD2525549;

  /**
   * Negative magic followed by a format version. A valid legacy payload starts with its nonnegative salt length, so
   * this header unambiguously distinguishes authenticated ciphertext from the legacy format.
   */
  private static final byte[] AUTHENTICATED_FORMAT_HEADER = {
      (byte) 0xD2, 0x52, 0x55, 0x49, 0x01
  };

  // User provided secret phrase used for encrypting data
  private final char[] passPhrase;

  // Variables for algorithm used to generate a SecretKey based on user provided passPhrase
  private final String secretKeyFactoryAlg;
  private final int saltSize;
  private final int iterationCount;
  private final int keyLength;

  // Cipher algorithm information retained for decrypting ciphertext written by earlier versions.
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

    // Validate authenticated parameters eagerly; the legacy transformation is decrypt-only and validated on first use.
    final String testString = "duh! !! !!!";
    Preconditions.checkState(
        testString.equals(StringUtils.fromUtf8(decrypt(encrypt(StringUtils.toUtf8(testString))))),
        "decrypt(encrypt(testString)) failed"
    );
  }

  public byte[] encrypt(byte[] plain)
  {
    try {
      final byte[] salt = new byte[saltSize];
      SECURE_RANDOM_INSTANCE.nextBytes(salt);

      final SecretKey tmp = getKeyFromPassword(passPhrase, salt);
      final SecretKey secret = new SecretKeySpec(tmp.getEncoded(), AUTHENTICATED_CIPHER_ALGORITHM);

      final byte[] iv = new byte[GCM_IV_SIZE];
      SECURE_RANDOM_INSTANCE.nextBytes(iv);

      final Cipher ecipher = Cipher.getInstance(AUTHENTICATED_CIPHER_TRANSFORMATION);
      ecipher.init(Cipher.ENCRYPT_MODE, secret, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
      ecipher.updateAAD(AUTHENTICATED_FORMAT_HEADER);

      final byte[] encryptedData = new EncryptedData(
          salt,
          iv,
          ecipher.doFinal(plain)
      ).toByteAray();
      return ByteBuffer.allocate(Math.addExact(AUTHENTICATED_FORMAT_HEADER.length, encryptedData.length))
                       .put(AUTHENTICATED_FORMAT_HEADER)
                       .put(encryptedData)
                       .array();
    }
    catch (Exception ex) {
      log.noStackTrace().warn(ex, "Encryption failed");
      throw InternalServerError.exception("Encryption failed. Check service logs.");
    }
  }

  public byte[] decrypt(byte[] data)
  {
    try {
      if (hasAuthenticatedFormatMagic(data)) {
        return decryptAuthenticated(data);
      }

      final EncryptedData encryptedData = EncryptedData.fromByteArray(data);

      final SecretKey tmp = getKeyFromPassword(passPhrase, encryptedData.getSalt());
      final SecretKey secret = new SecretKeySpec(tmp.getEncoded(), cipherAlgName);

      // error-prone warns if the transformation is not a compile-time constant
      // since it cannot check it for insecure combinations.
      // Legacy ciphertext may use a weaker configured transformation; new ciphertext is always written using GCM.
      @SuppressWarnings({
          "InsecureCryptoUsage",
          "codeql[java/potentially-weak-cryptographic-algorithm]"
      })
      final Cipher dcipher = Cipher.getInstance(transformation);
      dcipher.init(Cipher.DECRYPT_MODE, secret, new IvParameterSpec(encryptedData.getIv()));
      return dcipher.doFinal(encryptedData.getCipher());
    }
    catch (Exception ex) {
      log.noStackTrace().warn(ex, "Decryption failed");
      throw InternalServerError.exception("Decryption failed. Check service logs.");
    }
  }

  private byte[] decryptAuthenticated(final byte[] data) throws Exception
  {
    Preconditions.checkArgument(
        data.length >= AUTHENTICATED_FORMAT_HEADER.length
        && Arrays.equals(
            data,
            0,
            AUTHENTICATED_FORMAT_HEADER.length,
            AUTHENTICATED_FORMAT_HEADER,
            0,
            AUTHENTICATED_FORMAT_HEADER.length
        ),
        "Unsupported encrypted data version"
    );

    final EncryptedData encryptedData = EncryptedData.fromByteArray(
        Arrays.copyOfRange(data, AUTHENTICATED_FORMAT_HEADER.length, data.length)
    );
    Preconditions.checkArgument(encryptedData.getIv().length == GCM_IV_SIZE, "Invalid GCM IV size");

    final SecretKey tmp = getKeyFromPassword(passPhrase, encryptedData.getSalt());
    final SecretKey secret = new SecretKeySpec(tmp.getEncoded(), AUTHENTICATED_CIPHER_ALGORITHM);
    final Cipher dcipher = Cipher.getInstance(AUTHENTICATED_CIPHER_TRANSFORMATION);
    dcipher.init(
        Cipher.DECRYPT_MODE,
        secret,
        new GCMParameterSpec(GCM_TAG_LENGTH_BITS, encryptedData.getIv())
    );
    dcipher.updateAAD(AUTHENTICATED_FORMAT_HEADER);
    return dcipher.doFinal(encryptedData.getCipher());
  }

  private static boolean hasAuthenticatedFormatMagic(final byte[] data)
  {
    return data.length >= Integer.BYTES
           && ByteBuffer.wrap(data).getInt() == AUTHENTICATED_FORMAT_MAGIC;
  }

  private SecretKey getKeyFromPassword(final char[] passPhrase, final byte[] salt)
      throws NoSuchAlgorithmException, InvalidKeySpecException
  {
    final SecretKeyFactory factory = SecretKeyFactory.getInstance(secretKeyFactoryAlg);
    final KeySpec spec = new PBEKeySpec(passPhrase, salt, iterationCount, keyLength);
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
      final int headerLength = 12;
      final int encryptedDataLength =
          Math.addExact(Math.addExact(Math.addExact(salt.length, iv.length), cipher.length), headerLength);
      final ByteBuffer bb = ByteBuffer.allocate(encryptedDataLength);
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
      Preconditions.checkArgument(array.length >= 12, "Invalid encrypted data");
      final ByteBuffer bb = ByteBuffer.wrap(array);

      final int saltSize = bb.getInt();
      final int ivSize = bb.getInt();
      final int cipherSize = bb.getInt();
      final long payloadSize = (long) saltSize + ivSize + cipherSize;
      Preconditions.checkArgument(
          saltSize >= 0 && ivSize >= 0 && cipherSize >= 0 && payloadSize == bb.remaining(),
          "Invalid encrypted data"
      );

      final byte[] salt = new byte[saltSize];
      bb.get(salt);

      final byte[] iv = new byte[ivSize];
      bb.get(iv);

      final byte[] cipher = new byte[cipherSize];
      bb.get(cipher);

      return new EncryptedData(salt, iv, cipher);
    }
  }
}
