package org.apache.druid.indexing.kinesis.aws;

import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.PasswordProvider;

public class ConstructibleAWSCredentialsConfig extends AWSCredentialsConfig
{
  final private String accessKey;
  final private String secretKey;
  final private String fileSessionCredentials;

  public ConstructibleAWSCredentialsConfig(String accessKey, String secretKey)
  {
    this(accessKey, secretKey, null);
  }

  public ConstructibleAWSCredentialsConfig(String accessKey, String secretKey, String fileSessionCredentials)
  {
    this.accessKey = accessKey != null ? accessKey : "";
    this.secretKey = secretKey != null ? secretKey : "";
    this.fileSessionCredentials = fileSessionCredentials != null ? fileSessionCredentials : "";
  }

  @Override
  public PasswordProvider getAccessKey()
  {
    return DefaultPasswordProvider.fromString(accessKey);
  }

  @Override
  public PasswordProvider getSecretKey()
  {
    return DefaultPasswordProvider.fromString(secretKey);
  }

  @Override
  public String getFileSessionCredentials() { return fileSessionCredentials; }
}
