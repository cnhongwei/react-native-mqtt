package com.cnhongwei.pahoprobe;

import android.app.Activity;
import android.os.Bundle;
import android.util.Log;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import java.nio.charset.StandardCharsets;

public class MainActivity extends Activity {
  private static final String TAG = "MQTT_PROBE";
  @Override public void onCreate(Bundle state) { super.onCreate(state); new Thread(() -> { runTest("tcp", "tcp://10.0.2.2:1883"); runTest("ws", "ws://10.0.2.2:8080/mqtt"); }).start(); }
  private void runTest(String transport, String uri) {
    MqttAsyncClient client = null;
    try { client = new MqttAsyncClient(uri, "paho-" + System.nanoTime(), new MemoryPersistence()); MqttConnectOptions options = new MqttConnectOptions(); options.setCleanSession(true); client.connect(options).waitForCompletion(); client.publish("probe/paho/" + transport, ("paho-" + transport).getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(); client.disconnect().waitForCompletion(); Log.i(TAG, "RESULT client=Paho transport=" + transport + " uri=" + uri + " PASS"); }
    catch (Throwable e) { Log.e(TAG, "RESULT client=Paho transport=" + transport + " uri=" + uri + " FAIL " + e.getClass().getSimpleName() + ":" + e.getMessage()); }
  }
}
