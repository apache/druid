package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SchemaFingerprintGenerator
{

  private final ObjectMapper objectMapper;

  @Inject
  public SchemaFingerprintGenerator(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  public String generateId(Object payload)
  {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] serializedObj = objectMapper.writeValueAsBytes(payload);

      digest.update(serializedObj);
      byte[] hashBytes = digest.digest();
      return bytesToHex(hashBytes);
    } catch (NoSuchAlgorithmException | IOException e) {
      throw new RuntimeException("Error generating object hash", e);
    }
  }

  private String bytesToHex(byte[] bytes) {
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
