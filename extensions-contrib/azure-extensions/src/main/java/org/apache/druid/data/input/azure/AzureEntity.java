package org.apache.druid.data.input.azure;

import com.google.common.base.Predicate;
import org.apache.druid.data.input.RetryingInputEntity;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.storage.azure.AzureByteSource;
import org.apache.druid.storage.azure.AzureStorage;
import org.apache.druid.storage.azure.AzureUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class AzureEntity extends RetryingInputEntity
{
  private final CloudObjectLocation location;
  private final AzureByteSource byteSource;

  AzureEntity(CloudObjectLocation location, AzureStorage storage)
  {
    this.location = location;
    this.byteSource = new AzureByteSource(storage, location.getBucket(), location.getPath());
  }

  @Nullable
  @Override
  public URI getUri()
  {
    return location.toUri(AzureStorageInputSource.SCHEME);
  }

  @Override
  protected InputStream readFrom(long offset) throws IOException
  {
    // Get data of the given object and open an input stream
    return byteSource.openStream(offset);
  }

  @Override
  protected String getPath()
  {
    return location.getPath();
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return AzureUtils.AZURE_RETRY;
  }
}
