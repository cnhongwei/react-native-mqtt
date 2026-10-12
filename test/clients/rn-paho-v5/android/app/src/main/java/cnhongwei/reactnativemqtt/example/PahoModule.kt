package cnhongwei.reactnativemqtt.example

import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.ReactContextBaseJavaModule
import com.facebook.react.bridge.ReactMethod
import android.util.Log
import org.eclipse.paho.mqttv5.client.MqttAsyncClient
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions
import org.eclipse.paho.mqttv5.client.MqttClientPersistence
import org.eclipse.paho.mqttv5.client.logging.LoggerFactory
import java.util.ResourceBundle

class PahoModule(context: ReactApplicationContext) : ReactContextBaseJavaModule(context) {
  companion object { init { LoggerFactory.setLogger(PahoLogger::class.java.name) } }
  private val tag = "PAHO_RN_AB"
  private var client: MqttAsyncClient? = null
  override fun getName() = "PahoProbe"
  @ReactMethod fun diagnose(promise: Promise) {
    try {
      val loggerClass = Class.forName(PahoLogger::class.java.name)
      loggerClass.getDeclaredConstructor().newInstance()
      Log.i(tag, "DIAG class-load-and-constructor PASS")
    } catch (error: Throwable) { Log.e(tag, "DIAG class-load-and-constructor FAIL", error) }
    try {
      val bundle = ResourceBundle.getBundle("org.eclipse.paho.mqttv5.client.internal.nls.logcat")
      Log.i(tag, "DIAG resource-bundle PASS keys=${bundle.keySet().size}")
    } catch (error: Throwable) { Log.e(tag, "DIAG resource-bundle FAIL", error) }
    try {
      LoggerFactory.setLogger(PahoLogger::class.java.name)
      LoggerFactory.getLogger(LoggerFactory.MQTT_CLIENT_MSG_CAT, "rn-paho-diagnostic")
      Log.i(tag, "DIAG logger-factory PASS")
    } catch (error: Throwable) { Log.e(tag, "DIAG logger-factory FAIL", error) }
    promise.resolve(null)
  }
  @ReactMethod fun connect(port: Double, transport: String, promise: Promise) {
    Thread {
      try {
        LoggerFactory.setLogger(PahoLogger::class.java.name)
        val uri = if (transport == "ws") "ws://10.0.2.2:${port.toInt()}/mqtt" else "tcp://10.0.2.2:${port.toInt()}"
        val next = MqttAsyncClient(uri, "rn-paho-v5-${System.nanoTime()}", null as MqttClientPersistence?)
        next.connect(MqttConnectionOptions().apply { isCleanStart = true }).waitForCompletion()
        client = next
        Log.i(tag, "RESULT transport=$transport port=${port.toInt()} PASS")
        promise.resolve(null)
      } catch (error: Throwable) { Log.e(tag, "RESULT transport=$transport port=${port.toInt()} FAIL ${error.javaClass.simpleName}:${error.message}", error); promise.reject("PahoConnectFailed", error.message, error) }
    }.start()
  }
  @ReactMethod fun disconnect(promise: Promise) {
    Thread { try { client?.disconnect()?.waitForCompletion(); client = null; promise.resolve(null) } catch (error: Throwable) { promise.reject("PahoDisconnectFailed", error.message, error) } }.start()
  }
}
