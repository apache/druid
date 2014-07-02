package io.druid.storage.s3;

import com.amazonaws.auth.AWSSessionCredentials;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;

public class TestFileSessionCredentialsProvider {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void test() throws IOException {
    File file = folder.newFile();
    PrintWriter out = new PrintWriter(file.getAbsolutePath());
    out.println("sessionToken=sessionTokenSample\nsecretKey=secretKeySample\naccessKey=accessKeySample");
    out.close();

    FileSessionCredentialsProvider provider = new FileSessionCredentialsProvider(file.getAbsolutePath());
    AWSSessionCredentials sessionCredentials = (AWSSessionCredentials) provider.getCredentials();
    assertEquals(sessionCredentials.getSessionToken(), "sessionTokenSample");
    assertEquals(sessionCredentials.getAWSAccessKeyId(), "accessKeySample");
    assertEquals(sessionCredentials.getAWSSecretKey(), "secretKeySample");
  }
}
