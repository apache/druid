package com.metamx.phonebook;

import java.util.List;

/**
 * A PhoneBook object is just like a phone book.  You can publish ("announce") your services to it as well as
 * find out about other people publishing their services (registerListener).
 *
 * Finding out about other people's announcements is accomplished by employing a Peon, who gets notified of
 * announcements coming and going and does something with them.
 */
public interface PhoneBook
{
  public void start();
  public void stop();
  public boolean isStarted();
  public <T> void announce(String serviceName, String nodeName, T properties);
  public void unannounce(String serviceName, String nodeName);
  public <T> T lookup(String serviceName, Class<? extends T> clazz);
  public <T> void post(String serviceName, String nodeName, T properties);
  public boolean unpost(String serviceName, String nodeName);
  public <T> void postEphemeral(String serviceName, String nodeName, T properties);
  public <T> void registerListener(String serviceName, PhoneBookPeon<T> peon);
  public <T> void unregisterListener(String serviceName, PhoneBookPeon<T> peon);

  /**
   * A method to combine a number of hierarchical parts into a String that would "work" for this PhoneBook implementation.
   *
   * I.e., a call to combineParts("A", "B") should return the String "serviceName" that can be used to register a
   * listener underneath something that was announced via a call to announce("A", "B", {})
   *
   * @param parts
   * @return
   */
  public String combineParts(List<String> parts);
}
