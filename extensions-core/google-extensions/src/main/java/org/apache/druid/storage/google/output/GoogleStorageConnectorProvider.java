package org.apache.druid.storage.google.output;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleStorageDruidModule;

import javax.annotation.Nullable;
import java.io.File;

@JsonTypeName(GoogleStorageDruidModule.SCHEME)
public class GoogleStorageConnectorProvider extends GoogleOutputConfig implements StorageConnectorProvider
{

  @JacksonInject
  final GoogleStorage googleStorage;
  final GoogleInputDataConfig googleInputDataConfig;

  @JsonCreator
  public GoogleStorageConnectorProvider(
      @JacksonInject GoogleStorage storage,
      @JacksonInject GoogleInputDataConfig inputDataConfig,
      @JsonProperty(value = "bucket", required = true) String bucket,
      @JsonProperty(value = "prefix", required = true) String prefix,
      @JsonProperty(value = "tempDir", required = true) File tempDir,
      @JsonProperty(value = "chunkSize") @Nullable HumanReadableBytes chunkSize,
      @JsonProperty(value = "maxRetry") @Nullable Integer maxRetry
  )
  {
    super(bucket, prefix, tempDir, chunkSize, maxRetry);
    googleStorage = storage;
    googleInputDataConfig = inputDataConfig;

  }

  @Override
  public StorageConnector get()
  {
    return new GoogleStorageConnector(this, googleStorage, googleInputDataConfig);
  }
}
