package io.druid.storage.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.jets3t.service.security.AWSSessionCredentials;

public class AWSSessionCredentialsAdapter extends AWSSessionCredentials {
  private final AWSCredentialsProvider provider;

  public AWSSessionCredentialsAdapter(AWSCredentialsProvider provider) {
    super(null, null, null);
    this.provider = provider;
  }

  @Override
  protected String getTypeName() {
    return "AWSSessionCredentialsAdapter";
  }

  @Override
  public String getVersionPrefix() {
    return "Netflix AWSSessionCredentialsAdapter, version: ";
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
    if (provider.getCredentials() instanceof com.amazonaws.auth.AWSSessionCredentials) {
      com.amazonaws.auth.AWSSessionCredentials sessionCredentials =
          (com.amazonaws.auth.AWSSessionCredentials) provider.getCredentials();
      return sessionCredentials.getSessionToken();
    } else {
      return "";
    }
  }
}
