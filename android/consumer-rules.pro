# Paho selects its logger implementation by class name.
-keep class com.cnhongwei.reactnativemqtt.PahoLogger { public <init>(); *; }
-keep class org.eclipse.paho.client.mqttv3.logging.** { *; }
-keep class org.eclipse.paho.mqttv5.client.logging.** { *; }

# TLS providers and the explicitly selected insecure TrustManagerFactory are
# instantiated by name/provider registration.
-keep class com.cnhongwei.reactnativemqtt.InsecureTrustManagerFactorySpi { public <init>(); }
-keep class org.bouncycastle.jce.provider.BouncyCastleProvider { *; }
-keep class org.bouncycastle.jcajce.provider.** { *; }
