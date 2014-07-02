package io.druid.storage.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

public class FileSessionCredentialsProvider implements AWSCredentialsProvider {
  private final String sessionCredentials;
  private String sessionToken;
  private String accessKey;
  private String secretKey;

  public FileSessionCredentialsProvider(String sessionCredentials) {
    this.sessionCredentials = sessionCredentials;
    refresh();
  }

  @Override
  public AWSCredentials getCredentials() {
    return new AWSSessionCredentials() {
      @Override
      public String getSessionToken() {
        return sessionToken;
      }

      @Override
      public String getAWSAccessKeyId() {
        return accessKey;
      }

      @Override
      public String getAWSSecretKey() {
        return secretKey;
      }
    };
  }

  @Override
  public void refresh() {
    try {
      List<String> lines = Files.readAllLines(Paths.get(sessionCredentials), Charset.defaultCharset());
      Properties props = new Properties();
      for (String line : lines) {
        String[] tokens = line.split("=");
        props.put(tokens[0], tokens[1]);
      }

      sessionToken = props.getProperty("sessionToken");
      accessKey = props.getProperty("accessKey");
      secretKey = props.getProperty("secretKey");
    } catch (IOException e) {
      throw new RuntimeException("cannot refresh AWS credentials", e);
    }
  }
}
