package com.metamx.phonebook;

import org.apache.commons.codec.digest.DigestUtils;

import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;

/**
 */
public class ConsistentHashLookup implements ServiceLookup
{
  private final TreeMap<BigInteger, Map<String, String>> hashCircle = new TreeMap<BigInteger, Map<String, String>>();

  private final PhoneBook yp;
  private final int numVirtualNodes;

  private final Hasher hasher;

  public ConsistentHashLookup(
      PhoneBook yp,
      String serviceName
  )
  {
    this(yp, serviceName, 100);
  }

  public ConsistentHashLookup(
      PhoneBook yp,
      String serviceName,
      final int numVirtualNodes
  )
  {
    this.yp = yp;
    this.numVirtualNodes = numVirtualNodes;

    this.hasher = new Hasher();

    yp.registerListener(
        serviceName,
        new RingUpdaterPeon(
            hashCircle,
            numVirtualNodes,
            hasher
        )
    );
  }

  @Override
  public Map<String, String> get(String lookupKey)
  {
    if (hashCircle.isEmpty()) {
      return null;
    }

    BigInteger key = hasher.hash(lookupKey);

    Map.Entry<BigInteger, Map<String, String>> retEntry = hashCircle.ceilingEntry(key);

    if (retEntry == null) {
      retEntry = hashCircle.firstEntry();
    }

    return retEntry.getValue();
  }

  private static class Hasher
  {
    public BigInteger hash(String name)
    {
      return new BigInteger(DigestUtils.md5(name));
    }
  }

  private static class RingUpdaterPeon implements PhoneBookPeon<Map>
  {
    private final TreeMap<BigInteger,Map<String,String>> hashCircle;
    private final int numVirtualNodes;
    private final Hasher hasher;

    public RingUpdaterPeon(
        TreeMap<BigInteger, Map<String, String>> hashCircle,
        int numVirtualNodes,
        Hasher hasher
    )
    {
      this.hashCircle = hashCircle;
      this.numVirtualNodes = numVirtualNodes;
      this.hasher = hasher;
    }

    @Override
    public Class<Map> getObjectClazz()
    {
      return Map.class;
    }

    @Override
    public void newEntry(String name, Map properties)
    {
      for (int i = 0; i < numVirtualNodes; i++) {
        hashCircle.put(hasher.hash(name + i), properties);
      }
    }

    @Override
    public void entryRemoved(String name)
    {
      for (int i = 0; i < numVirtualNodes; i++) {
        hashCircle.remove(hasher.hash(name + i));
      }
    }
  }
}
