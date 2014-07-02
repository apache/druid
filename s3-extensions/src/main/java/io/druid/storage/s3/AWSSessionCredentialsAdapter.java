package io.druid.storage.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.jets3t.service.security.AWSSessionCredentials;

public class AWSSessionCredentialsAdapter extends AWSSessionCredentials {
  private final AWSCredentialsProvider provider;

  public AWSSessionCredentialsAdapter(AWSCredentialsProvider provider) {
    super(null, null, null);
    if(provider.getCredentials() instanceof com.amazonaws.auth.AWSSessionCredentials)
      this.provider = provider;
    else
      throw new IllegalArgumentException("provider does not contain session credentials");
  }

  @Override
  protected String getTypeName() {
    return "AWSSessionCredentialsAdapter";
  }

  @Override
  public String getVersionPrefix() {
    return "AWSSessionCredentialsAdapter, version: ";
  }

  @Override
  public String getAccessKey() {
    return provider.getCredentials().getAWSAccessKeyId();
  }

  @Override
  public String getSecretKey() {
    return provider.getCredentials().getAWSSecretKey();
  }

  public String getSessionToken() {
    com.amazonaws.auth.AWSSessionCredentials sessionCredentials =
        (com.amazonaws.auth.AWSSessionCredentials) provider.getCredentials();
    return sessionCredentials.getSessionToken();
  }
}
