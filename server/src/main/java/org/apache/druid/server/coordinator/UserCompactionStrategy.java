package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;

/**
 * Encapsulates the Engine to be used for a compaction task.
 * Should be synchronized with the types for {@link org.apache.druid.indexing.common.task.CompactionTasksLauncher}.
 */
public class UserCompactionStrategy
{
  private final CompactionEngine type;

  public UserCompactionStrategy(CompactionEngine type)
  {
    this.type = type;
  }

  @JsonProperty
  public CompactionEngine getType()
  {
    return type;
  }

  public enum CompactionEngine
  {
    NATIVE,
    MSQ;

    @JsonCreator
    public static CompactionEngine fromString(String name)
    {
      if (name == null) {
        return null;
      }
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  @Override
  public String toString()
  {
    return "UserCompactionStrategy{" +
           "type=" + type +
           '}';
  }
}
