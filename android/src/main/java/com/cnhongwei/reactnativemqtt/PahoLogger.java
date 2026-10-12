package com.cnhongwei.reactnativemqtt;

import java.util.ResourceBundle;

/**
 * Android-safe no-op logger for both direct Paho Java clients.
 *
 * Paho selects its logger by class name and otherwise loads a desktop logging
 * implementation reflectively. Keeping this small implementation in the
 * library avoids making Android consumers depend on that optional path.
 */
public final class PahoLogger
    implements org.eclipse.paho.client.mqttv3.logging.Logger,
               org.eclipse.paho.mqttv5.client.logging.Logger {
  public PahoLogger() {}
  public void initialise(ResourceBundle c, String id, String r) {}
  public void setResourceName(String r) {}
  public boolean isLoggable(int l) { return false; }
  public void severe(String a, String b, String c) {}
  public void severe(String a, String b, String c, Object[] d) {}
  public void severe(String a, String b, String c, Object[] d, Throwable e) {}
  public void warning(String a, String b, String c) {}
  public void warning(String a, String b, String c, Object[] d) {}
  public void warning(String a, String b, String c, Object[] d, Throwable e) {}
  public void info(String a, String b, String c) {}
  public void info(String a, String b, String c, Object[] d) {}
  public void info(String a, String b, String c, Object[] d, Throwable e) {}
  public void config(String a, String b, String c) {}
  public void config(String a, String b, String c, Object[] d) {}
  public void config(String a, String b, String c, Object[] d, Throwable e) {}
  public void fine(String a, String b, String c) {}
  public void fine(String a, String b, String c, Object[] d) {}
  public void fine(String a, String b, String c, Object[] d, Throwable e) {}
  public void finer(String a, String b, String c) {}
  public void finer(String a, String b, String c, Object[] d) {}
  public void finer(String a, String b, String c, Object[] d, Throwable e) {}
  public void finest(String a, String b, String c) {}
  public void finest(String a, String b, String c, Object[] d) {}
  public void finest(String a, String b, String c, Object[] d, Throwable e) {}
  public void log(int a, String b, String c, String d, Object[] e, Throwable f) {}
  public void trace(int a, String b, String c, String d, Object[] e, Throwable f) {}
  public String formatMessage(String a, Object[] b) { return a; }
  public void dumpTrace() {}
}
