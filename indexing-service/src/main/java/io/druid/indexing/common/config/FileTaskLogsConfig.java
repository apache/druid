package io.druid.indexing.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.io.File;

public class FileTaskLogsConfig
{
  @JsonProperty
  @NotNull
  private File directory = new File("log");

  public File getDirectory()
  {
    return directory;
  }
}
