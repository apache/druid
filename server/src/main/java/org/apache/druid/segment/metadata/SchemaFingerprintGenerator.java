package org.apache.druid.segment.metadata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SchemaFingerprintGenerator
{
  public static String generateId(Object payload)
  {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] serializedObj = serializeObject(payload);
      digest.update(serializedObj);
      byte[] hashBytes = digest.digest();
      return bytesToHex(hashBytes);
    } catch (NoSuchAlgorithmException | IOException e) {
      throw new RuntimeException("Error generating object hash", e);
    }
  }

  private static byte[] serializeObject(Object obj) throws IOException
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(obj);
    oos.close();
    return bos.toByteArray();
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder hexString = new StringBuilder();
    for (byte b : bytes) {
      String hex = Integer.toHexString(0xff & b);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    return hexString.toString();
  }
}
