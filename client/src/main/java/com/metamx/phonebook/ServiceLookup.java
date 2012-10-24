package com.metamx.phonebook;

import java.util.Map;

/**
 * A ServiceLookup is an object that, when given a key, will return a metadata map for that key.  This was created
 * for use in doing things like consistent hashing, where the lookupKey represents the partition key and the
 * metadata map has stuff like host and port in it (basically, the information required to be able to contact the server)
 */
public interface ServiceLookup
{
  public Map<String, String> get(String lookupKey);
}
