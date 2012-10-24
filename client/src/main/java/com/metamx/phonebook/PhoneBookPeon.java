package com.metamx.phonebook;

/**
 * A PhoneBookPeon is a Dilbert-like character who sits in his cubicle all day just waiting to hear about newEntry()s
 * and removed entries.  He acts as the go between, someone that the PhoneBook knows how to talk to and can then
 * translate said message into whatever his employer wants.
 */
public interface PhoneBookPeon<T>
{
  public Class<T> getObjectClazz();
  public void newEntry(String name, T properties);
  public void entryRemoved(String name);
}
