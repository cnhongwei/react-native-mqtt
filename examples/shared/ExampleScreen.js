import React, { useEffect, useRef, useState } from 'react';
import {
  FlatList,
  Modal,
  Pressable,
  SafeAreaView,
  StatusBar,
  StyleSheet,
  Text,
  useColorScheme,
  View,
} from 'react-native';
import {
  createMqttClient,
  decodeUtf8,
  encodeUtf8,
} from '@cnhongwei/react-native-mqtt';
import { appendBounded, clipUtf8Text, formatPublishText } from './demoHelpers';
import {
  demoConnectionOptions,
  protocolLabel,
  runProtocolDemo,
} from './protocolDemo';

export const DEMO_TOPIC = 'cnhongwei/react-native-mqtt';
export const MOSQUITTO_CA_BASE64 =
  'MIIEAzCCAuugAwIBAgIUBY1hlCGvdj4NhBXkZ/uLUZNILAwwDQYJKoZIhvcNAQELBQAwgZAxCzAJBgNVBAYTAkdCMRcwFQYDVQQIDA5Vbml0ZWQgS2luZ2RvbTEOMAwGA1UEBwwFRGVyYnkxEjAQBgNVBAoMCU1vc3F1aXR0bzELMAkGA1UECwwCQ0ExFjAUBgNVBAMMDW1vc3F1aXR0by5vcmcxHzAdBgkqhkiG9w0BCQEWEHJvZ2VyQGF0Y2hvby5vcmcwHhcNMjAwNjA5MTEwNjM5WhcNMzAwNjA3MTEwNjM5WjCBkDELMAkGA1UEBhMCR0IxFzAVBgNVBAgMDlVuaXRlZCBLaW5nZG9tMQ4wDAYDVQQHDAVEZXJieTESMBAGA1UECgwJTW9zcXVpdHRvMQswCQYDVQQLDAJDQTEWMBQGA1UEAwwNbW9zcXVpdHRvLm9yZzEfMB0GCSqGSIb3DQEJARYQcm9nZXJAYXRjaG9vLm9yZzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAME0HKmIzfTOwkKLT3THHe+ObdizamPgUZmD64Tf3zJdNeYGYn4CEXbyP6fy3tWc8S2boW6dzrH8SdFf9uo320GJA9B7U1FWTe3xda/Lm3JFfaHjkWw7jBwcauQZjpGINHapHRlpiCZsquAthOgxW9SgDgYlGzEAs06pkEFiMw+qDfLo/sxFKB6vQlFekMeCymjLCbNwPJyqyhFmPWwio/PDMruBTzPH3cioBnrJWKXc3OjXdLGFJOfj7pP0j/dr2LH72eSvv3PQQFl90CZPFhrCUcRHSSxoE6yjGOdnz7f6PveLIB574kQORwt8ePn0yidrTC1ictikED3nHYhMUOUCAwEAAaNTMFEwHQYDVR0OBBYEFPVV6xBUFPiGKDyo5V3+Hbh4N9YSMB8GA1UdIwQYMBaAFPVV6xBUFPiGKDyo5V3+Hbh4N9YSMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAGa9kS21N70ThM6/Hj9D7mbVxKLBjVWe2TPsGfbl3rEDfZ+OKRZ2j6AC6r7jb4TZO3dzF2p6dgbrlU71Y/4K0TdzIjRj3cQ3KSm41JvUQ0hZ/c04iGDg/xWf+pp58nfPAYwuerruPNWmlStWAXf0UTqRtg4hQDWBuUFDJTuWuuBvEXudz74eh/wKsMwfu1HFvjy5Z0iMDU8PUDepjVolOCue9ashlS4EB5IECdSR2TItnAIiIwimx839LdUdRudafMu5T5Xma182OC0/u/xRlEm+tvKGGmfFcN0piqVl8OrSPBgIlb+1IKJEm/XriWr/Cq4h/JfB7NTsezVslgkBaoU=';
export const CLIENT_P12_BASE64 =
  'MIIKZwIBAzCCChUGCSqGSIb3DQEHAaCCCgYEggoCMIIJ/jCCBGoGCSqGSIb3DQEHBqCCBFswggRXAgEAMIIEUAYJKoZIhvcNAQcBMF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBCVFWlydxKrJmAJMtoIgCbyAgIIADAMBggqhkiG9w0CCQUAMB0GCWCGSAFlAwQBKgQQmENSUDMxM0As3MPbZQa8JYCCA+Acc3y4TLkE96wiLklW49wD85icWF5opHj9QbR0BL8lfoy9Vbfu63SqSE9jWnY9cuJgvuvQxUCRBibRcZrSEVi5ZNAwYU50Wky5EREvAdKp5ZRTR/EexFg/ztJSIb9JQrMagF7Cg7M/3EQ8ZfzBwqjniFiBaeFXQto4nr9D2RCzph2OOTfFSaDxQDjfp28VuEbPsFEMhc+hFqHsiFxgztYivNqr9fzZ/jtEvbwn1VOKCi9ccmOtrG5BFxGNKwwKSZ3k6+9VmG0583FOBpRyjNHmq76YybCFqHqjZ4KEH3yUnAbC2lWuTFZ1c9BZ9ezy4IFdsyJ5HRl91NWdytY4TKSTBAt9Jp8C4GFLFNLoAFC4V4lpOvbCQo81QBhAhvbtX/szwxWjRGGOZEUg3DXRSSmf3pUqi+GIiT/hradQxxd+hXkBENt+6xEI7JwwcgNuT9sNWXKT/c1Kt3CZngC70EIVlsAbTTJj6N5/iqIOFaNk20Kaw7Q3BVAy80LH2MO9kE1Iek1CyAeKSs0rUkiMSKQotTP9YIZIQ/+UBpGFu/k/YAZDTLcP0IqQvXXmrPnm+qmjscheT8nKGF3UbesGsMXRW5SDdZW+NRE1N8EzezEbghiWm8Hls0tlobAR3xd/S6fiJH2eufUJYKk9k7oifjg1Qi+9hGVi7YWrYyjOyEaQ5TTpsbuyr7SiFtBFLeigf1QCT0Mycbrvt1ulBFnlE/gYo1SrubLQH7TblREHNCGXKIdAib8lSJ5uZe39rkYcA7fD69PpY1mzMMJl+L/HZ8s3F8gSUL2kiAltRzh9yih2s7v+WFVnlgA0BwAwagSyj97vsvLvrB5T9iNu2Pd+NFx035Co2TCroJVwPexg/S4FivBsqx1v8FMzfIpBhkxdGMbW40SoKRYu2AW+YSRntDC+ST3869KfOZEjHUbGZXzuR78xpey0SVydSt6cQ4CZ81R2zuECby30is4jKwmHpq/s712DN420/PEiFjhyB90Oiq91fNR/pgNsiFkVG+TjxL25YSHGBpcuuE3agmt+9zAMCrnJTEhMqCe+bevms3lsBwzfqrihPsszF5KKPRuCbIM3vUOZAA34FDPJhijZN9WUI+LyOoMXkxSHA+ZCs0vjQBeB2ZYV/IPpLeWb4QVFgNGPUnz9uPqOGnDELCR7zXAvFoYExNIYrPPe4eGevfBa6sEFTS++OAWPY//dYaZUXzfZ6PAl/Ccn0mMr6PLmmw7lN9QyX+x8ISJMaZHPpv1QhA7vvs6uiZxzvTvsszL/65X/5a0KqlOQty+6+ZolU1fOBskgwK8Y02CQdgo8T7KuETCCBYwGCSqGSIb3DQEHAaCCBX0EggV5MIIFdTCCBXEGCyqGSIb3DQEMCgECoIIFOTCCBTUwXwYJKoZIhvcNAQUNMFIwMQYJKoZIhvcNAQUMMCQEEOtB89vkF8fUwfOONUgN//ACAggAMAwGCCqGSIb3DQIJBQAwHQYJYIZIAWUDBAEqBBAwH9Li7lMmnuRw+XUlIqtsBIIE0FiCJmr3RopD1LJzGv0w/ggOUjqYBeZNbFZL7YfkitpfZJZ2jJUmoJomno3uI2oIjgkhbImMEMFELaygnC/L0Oe6t9FKXIm3BODVcmFwP6gVoMX9Gu1THzC/zaBD+9ML2/Prr/fBnV8p7kL7BuVy1tvWEP9fWrB69REwk/ehVgVfbhhJ1Ptr/8t+dBraaZtqNL4QTt1jvjpBgImcoSpTMSTFgkCyUr2Jwig9rhUI/Km84lvU7D+6XyU5AOr1HLs6ABoSGde8ElqLQl6l71E6uvbffzuMcFdFtWjp+bBdWa+9TVZhgPcNYSFLvLP6eJ2etg8F5pNvrjAUHwwMAi6NmOrAOC70sCvEgDBks4oYJKBrmaHYLp3Hm9++nj9RkUiZUMEujOgKvpmjN6Z+ojpYjPY2QeoM6Y+UMVSD+Axa9CupnzYCranQ/BnFI1yqT+IS/KqrmfDZgd/voI5Bgx+WPZn0pzbPJtE/OBPxbri94WW6yWj7b1NRFjFamTC9DinPUvlStKfJ4AhQvQi/BzR6vDuh22iXTmPqsbPquVdgJykTUoQyiTy1gFNM4Gh5Y3oRMLTg75qlhA6Ba9cSN1XC+S/VwLO2C1C9bZhqhIiS/vAZc0v75Yu//sEvole9b0JihdfY6gai/tlvjvIlJVVNDjuZ7WC6P5s9iTqQbdsAP1smBUagGL9m2gYoE/6QVP3e9XgxlL8J5fFOg5p87SH0A+rBTypfAUC161dHni3985xifRZY17U3/xXJs3lznIe3CLz+e1GZE4PDFHm6v+y2UsZAIbzBLmmNnZ5ZWj573HMQRkRlzTsI905XS/3X9dO+fPF2K2fTzPLgzjAkUXp+RUHm4oZ7ToXIyMXIH09uZvIfCAB2sKl23Sb1STRDi2b38DqB+fd1YjpCYBu4a8NLicWrl4ykGiuBU9nhTW71ktnUvHR9vcMJyzXs98tmoe8DWd+jYSwK4iPTjhgIlHSVJdNuLPg9RIk9hRzCxmHAB1nRyw/U1BJb8t7fdZe9zEgpk6s5wLCm4w+hHvPKAaYbpR7XoO7YfKApnWUIxmxHMNQlwQsJVzBqSLeTemlYM3BW57TaEAW3yO1r05Jq2VC5ROlae5ESvHpsyJvMtgutkp4U4wOIGURjVQl2apc8Z+ETvO9gsYHJ75kC4s3glLpItNVIrpkmWqh5JwcMQ/rSyehHYqArH9IPhb3LgT0dOgO+Jj4zgFFOENgnuZDnTJO/YoQ4BVh0gF6ELj3gcYMWRRS3EE5um9dcfIaXWP2VBbyUzorCraq8ogUw3h3jqO8USASGFEVYhAI0GTdPb3EN9j219mPrR/72YrU2j8CQhkWs6Pu4HB5KEFni33xU6GkgW/RCM8IJMnfE7Gb9u5a1n+pRRz9VMaj6q8aahjHYYJ3FUrt96s2FhYaZ8fLiRGNMoeVIqzj51sDtXEz/S0yzytiLKEX4ao0o7uKEpxf/ffjTG8MuYTA6+bpHnQMIO3HmxxoYQD0Mo+S4tO7FIB0PM45WA95i1h6Xbe+/lgEYrSrVacwBhS/wuuXtYojv+g/3LrjRkpGSvW4GtU2f4Q6tVsRLEIpol2BekZhG4L173MQozq0RH32pppACZHOW7Q4NdKYILuItZQT5lJFb7tIIEACLMSUwIwYJKoZIhvcNAQkVMRYEFCA19ZM5F6PeofExQtN84Tt7quPZMEkwMTANBglghkgBZQMEAgEFAAQgnqdBHQ9Q4yjAuHUCh8HkpWw3OIPwKZR+W2PX1S94koIEEKSTxDZzZtYHl9EOQl6zcqQCAggA';
export const CLIENT_P12_PASSWORD = '12345678';

const profiles = [
  { port: 1883, transport: 'tcp', label: 'TCP anonymous', auth: 'anonymous' },
  {
    port: 1884,
    transport: 'tcp',
    label: 'TCP read/write',
    auth: 'rw / readwrite',
    username: 'rw',
    password: 'readwrite',
  },
  {
    port: 8883,
    transport: 'tls',
    label: 'TLS anonymous',
    auth: 'anonymous',
    ca: true,
    allowInvalidHostnameForTesting: true,
  },
  {
    port: 8884,
    transport: 'tls',
    label: 'TLS mTLS',
    auth: 'client certificate',
    ca: true,
    p12: true,
    allowInvalidHostnameForTesting: true,
  },
  {
    port: 8885,
    transport: 'tls',
    label: 'TLS read/write',
    auth: 'rw / readwrite',
    username: 'rw',
    password: 'readwrite',
    ca: true,
    allowInvalidHostnameForTesting: true,
  },
  { port: 8886, transport: 'tls', label: 'TLS system CA', auth: 'anonymous' },
  {
    port: 8887,
    transport: 'tls',
    label: 'TLS expired certificate',
    auth: 'anonymous',
    ca: true,
    expectedFailure: true,
    expectedFailureReason: 'certificate expired',
  },
  {
    port: 8080,
    transport: 'ws',
    label: 'WebSocket anonymous',
    auth: 'anonymous',
    path: '/mqtt',
  },
  {
    port: 8081,
    transport: 'wss',
    label: 'WebSocket TLS anonymous',
    auth: 'anonymous',
    path: '/mqtt',
  },
  {
    port: 8090,
    transport: 'ws',
    label: 'WebSocket read/write',
    auth: 'rw / readwrite',
    username: 'rw',
    password: 'readwrite',
    path: '/mqtt',
  },
  {
    port: 8091,
    transport: 'wss',
    label: 'WebSocket TLS read/write',
    auth: 'rw / readwrite',
    username: 'rw',
    password: 'readwrite',
    path: '/mqtt',
  },
];
const time = () => new Date().toLocaleTimeString();
const messageMetadata = (value, encode, decode) =>
  clipUtf8Text(
    [
      `QoS ${value.qos}`,
      `retained ${String(value.retained)}`,
      `${value.bytes.length} bytes`,
      value.contentType ? `contentType ${value.contentType}` : '',
      value.responseTopic ? `responseTopic ${value.responseTopic}` : '',
      value.messageExpiryIntervalSeconds !== undefined
        ? `expiry ${value.messageExpiryIntervalSeconds}s`
        : '',
      value.payloadFormatIndicator !== undefined
        ? `payloadFormat ${value.payloadFormatIndicator}`
        : '',
      value.userProperties?.length
        ? `userProperties ${JSON.stringify(value.userProperties)}`
        : '',
    ]
      .filter(Boolean)
      .join(' · '),
    4096,
    encode,
    decode
  );
const details = (value) =>
  value && typeof value === 'object'
    ? {
        message: value.message,
        code: value.code,
        operation: value.operation,
        reasonCode: value.reasonCode,
        reasonString: value.reasonString,
      }
    : { message: String(value) };

export default function ExampleScreen({
  title,
  clientIdPrefix,
  runtimeConfig,
}) {
  const dark = useColorScheme() === 'dark';
  const c = dark
    ? {
        bg: '#101114',
        surface: '#1b1d22',
        text: '#f4f5f7',
        muted: '#adb3bf',
        border: '#3b404b',
        primary: '#8ab4ff',
        disabledButton: '#30343d',
        disabledText: '#adb3bf',
        danger: '#ff8a8a',
        overlay: 'rgba(0,0,0,.68)',
      }
    : {
        bg: '#f7f8fa',
        surface: '#fff',
        text: '#17181b',
        muted: '#5f6672',
        border: '#d9dde5',
        primary: '#155eef',
        disabledButton: '#d0d5dd',
        disabledText: '#475467',
        danger: '#c62828',
        overlay: 'rgba(0,0,0,.38)',
      };
  const s = styles(c);
  const cleanupRef = useRef();
  const clientRef = useRef();
  const subscriptionRef = useRef();
  const tokenRef = useRef(0);
  const sequenceRef = useRef(0);
  const testAbortRef = useRef();
  const [protocolVersion, setProtocolVersion] = useState(
    runtimeConfig?.connectionOptions?.protocolVersion ?? 5
  );
  const [testResult, setTestResult] = useState('');
  const availableProfiles = runtimeConfig?.profiles || profiles;
  const subscribeOptions = runtimeConfig?.subscribeOptions || { qos: 1 };
  const publishOptions = runtimeConfig?.publishOptions || { qos: 1 };
  const jsBlockPublishCount = runtimeConfig?.jsBlockPublishCount ?? 32;
  const [selected, setSelected] = useState(availableProfiles[0]);
  const [state, setState] = useState('idle');
  const [logs, setLogs] = useState([]);
  const [messages, setMessages] = useState([]);
  const [tab, setTab] = useState('logs');
  const [sheetOpen, setSheetOpen] = useState(false);
  const [pending, setPending] = useState('');
  const [error, setError] = useState('');
  const addLog = (kind, detail) =>
    setLogs((items) =>
      appendBounded(items, {
        id: ++sequenceRef.current,
        at: time(),
        kind,
        detail: clipUtf8Text(detail, 4096, encodeUtf8, decodeUtf8),
        port: selected.port,
        protocolVersion,
      })
    );
  const makeClient = (profile) => {
    const token = ++tokenRef.current;
    const tls = profile.transport === 'tls' || profile.transport === 'wss';
    const client = createMqttClient({
      host: 'test.mosquitto.org',
      port: profile.port,
      transport: profile.transport,
      path: profile.path,
      username: profile.username,
      password: profile.password,
      tls: tls
        ? {
            insecure: false,
            caBase64: profile.ca ? MOSQUITTO_CA_BASE64 : undefined,
            p12Base64: profile.p12 ? CLIENT_P12_BASE64 : undefined,
            p12Password: profile.p12 ? CLIENT_P12_PASSWORD : undefined,
            allowInvalidHostnameForTesting:
              profile.allowInvalidHostnameForTesting,
          }
        : undefined,
      clientId: `${clientIdPrefix}-${profile.port}-${Date.now()}`,
      ...demoConnectionOptions(
        runtimeConfig?.connectionOptions,
        protocolVersion
      ),
    });
    clientRef.current = client;
    const stateListener = client.onStateChange((snapshot) => {
      if (token === tokenRef.current) {
        setState(snapshot.state);
        addLog('state', snapshot.state);
      }
    });
    const messageListener = client.onMessage((value) => {
      if (token !== tokenRef.current) return;
      let text;
      try {
        text = decodeUtf8(value.bytes);
      } catch {
        text = `base64 bytes (${value.bytes.length})`;
      }
      setMessages((items) =>
        appendBounded(items, {
          id: ++sequenceRef.current,
          at: time(),
          topic: value.topic,
          text: clipUtf8Text(text, 16 * 1024, encodeUtf8, decodeUtf8),
          qos: value.qos,
          retained: value.retained,
          bytes: value.bytes.length,
          metadata: messageMetadata(value, encodeUtf8, decodeUtf8),
        })
      );
      addLog(
        'message',
        `${value.topic} · ${value.bytes.length} bytes received`
      );
    });
    const diagnosticListener = client.onDiagnostic((diagnostic) => {
      if (token === tokenRef.current)
        addLog('diagnostic', `${diagnostic.code}: ${diagnostic.message}`);
    });
    client.__cleanup = () => {
      stateListener.remove();
      messageListener.remove();
      diagnosticListener.remove();
    };
    return client;
  };
  const disposeCurrent = async (reason) => {
    testAbortRef.current?.abort();
    const client = clientRef.current;
    const subscription = subscriptionRef.current;
    subscriptionRef.current = undefined;
    clientRef.current = undefined;
    ++tokenRef.current;
    if (subscription) {
      try {
        await subscription.unsubscribe();
        addLog('unsubscribe', 'success');
      } catch (value) {
        addLog('unsubscribe', JSON.stringify(details(value)));
      }
    }
    if (client) {
      try {
        await client.disconnect();
        addLog('disconnect', reason || 'success');
      } catch (value) {
        addLog('disconnect', JSON.stringify(details(value)));
      }
      try {
        await client.dispose();
        addLog('dispose', reason || 'success');
      } catch (value) {
        addLog('dispose', JSON.stringify(details(value)));
      }
      client.__cleanup?.();
    }
    setState('idle');
  };
  cleanupRef.current = disposeCurrent;
  useEffect(
    () => () => {
      cleanupRef.current('unmount').catch(() => undefined);
    },
    []
  );
  const connect = async () => {
    if (
      pending ||
      clientRef.current ||
      ['connecting', 'connected', 'reconnecting'].includes(state)
    )
      return;
    setPending('connect');
    setError('');
    addLog('connect', 'start');
    try {
      const client = makeClient(selected);
      await client.connect();
      addLog('connect', 'success');
      subscriptionRef.current = await client.subscribe(
        DEMO_TOPIC,
        subscribeOptions
      );
      addLog('subscribe', DEMO_TOPIC);
      await client.ready();
      addLog('ready', 'subscription ready');
    } catch (value) {
      const d = details(value);
      const expected = selected.expectedFailure
        ? `expected TLS failure: ${selected.expectedFailureReason}`
        : '';
      setError(expected ? `${expected} · ${d.message}` : d.message);
      addLog(
        'error',
        JSON.stringify(expected ? { ...d, expectedFailure: expected } : d)
      );
      await disposeCurrent('connect failed');
    } finally {
      setPending('');
    }
  };
  const publish = async () => {
    const client = clientRef.current;
    if (!client || state !== 'connected' || pending) return;
    const text = formatPublishText();
    setPending('publish');
    addLog('publish', `start · ${text}`);
    try {
      await client.publishText(DEMO_TOPIC, text, publishOptions);
      addLog('publish', 'success');
    } catch (value) {
      const d = details(value);
      setError(d.message);
      addLog('error', JSON.stringify(d));
    } finally {
      setPending('');
    }
  };
  const runTest = async (properties = false) => {
    const client = clientRef.current;
    if (
      !client ||
      state !== 'connected' ||
      pending ||
      (properties && protocolVersion !== 5)
    )
      return;
    const token = tokenRef.current;
    const controller = new AbortController();
    testAbortRef.current = controller;
    setPending('test');
    setError('');
    setTestResult('Running…');
    try {
      await runProtocolDemo(client, {
        protocolVersion,
        properties,
        encode: encodeUtf8,
        signal: controller.signal,
        topic: `${DEMO_TOPIC}/checks/${Date.now()}-${Math.random().toString(36).slice(2)}`,
        onProgress: (result) => {
          if (token === tokenRef.current) addLog('test', result);
        },
      });
      if (token === tokenRef.current) {
        const result = `PASS · ${protocolLabel(protocolVersion)} · ${properties ? 'User Properties + Content Type' : 'QoS 0/1/2'} · unsubscribed`;
        setTestResult(result);
        addLog('test', result);
      }
    } catch (value) {
      if (token === tokenRef.current) {
        const result = `FAIL · ${protocolLabel(protocolVersion)} · ${value.message}`;
        setTestResult(result);
        addLog('test', result);
      }
    } finally {
      if (token === tokenRef.current) setPending('');
      if (testAbortRef.current === controller) testAbortRef.current = undefined;
    }
  };
  const chooseProtocol = async (version) => {
    if (
      pending ||
      ['connecting', 'connected', 'reconnecting', 'disconnecting'].includes(
        state
      )
    )
      return;
    setPending('protocol');
    try {
      if (clientRef.current) await disposeCurrent('protocol change');
      setProtocolVersion(version);
      setTestResult('');
      setError('');
    } finally {
      setPending('');
    }
  };
  const burst = async () => {
    const client = clientRef.current;
    if (!client || state !== 'connected' || pending) return;
    setPending('burst');
    addLog('js-block', `start · ${jsBlockPublishCount} publishes, 3 seconds`);
    const promises = Array.from({ length: jsBlockPublishCount }, (_, i) =>
      Promise.resolve().then(() =>
        client.publishText(DEMO_TOPIC, `burst-${i}`, publishOptions)
      )
    );
    const started = Date.now();
    while (Date.now() - started < 3000) {}
    const result = await Promise.allSettled(promises);
    addLog(
      'js-block',
      `complete · ${Date.now() - started}ms · ok=${result.filter((x) => x.status === 'fulfilled').length}/${jsBlockPublishCount} failed=${result.filter((x) => x.status === 'rejected').length}`
    );
    setPending('');
  };
  const disconnect = async () => {
    const client = clientRef.current;
    if (!client || pending) return;
    setPending('disconnect');
    addLog('disconnect', 'start');
    const subscription = subscriptionRef.current;
    subscriptionRef.current = undefined;
    if (subscription) {
      try {
        await subscription.unsubscribe();
        addLog('unsubscribe', 'success');
      } catch (value) {
        const d = details(value);
        setError(d.message);
        addLog('unsubscribe', JSON.stringify(d));
      }
    }
    try {
      await client.disconnect();
      addLog('disconnect', 'success');
      setState('disconnected');
    } catch (value) {
      const d = details(value);
      setError(d.message);
      addLog('error', JSON.stringify(d));
    } finally {
      setPending('');
    }
  };
  const dispose = async () => {
    if (!clientRef.current || pending) return;
    setPending('dispose');
    try {
      await disposeCurrent('user');
    } catch (value) {
      const d = details(value);
      setError(d.message);
      addLog('error', JSON.stringify(d));
    } finally {
      setPending('');
    }
  };
  const choose = async (profile) => {
    if (
      ['connecting', 'connected', 'reconnecting', 'disconnecting'].includes(
        state
      ) ||
      pending
    ) {
      setError('Disconnect before changing profile');
      addLog('profile', 'change rejected while client is active');
      return;
    }
    if (clientRef.current) {
      setPending('dispose');
      try {
        await disposeCurrent('profile change');
      } finally {
        setPending('');
      }
    }
    setSelected(profile);
    setTestResult('');
    setSheetOpen(false);
    addLog('profile', profile.label);
  };
  const renderLog = ({ item }) => (
    <View style={s.card}>
      <Text style={s.titleText}>
        {item.at} · {item.kind} · {protocolLabel(item.protocolVersion)}
      </Text>
      <Text style={s.text}>
        {clipUtf8Text(item.detail, 4096, encodeUtf8, decodeUtf8)}
      </Text>
    </View>
  );
  const renderMessage = ({ item }) => (
    <View style={s.card}>
      <Text style={s.titleText}>
        {item.at} · {item.topic}
      </Text>
      <Text style={s.text}>{item.text}</Text>
      <Text style={s.meta}>{item.metadata}</Text>
    </View>
  );
  const renderList = (name, data, renderItem) => (
    <FlatList
      testID={`mqtt-${name}`}
      data={[...data].reverse()}
      keyExtractor={(item) => String(item.id)}
      renderItem={renderItem}
      style={tab === name ? s.activeList : s.hiddenList}
      contentContainerStyle={data.length ? s.list : s.emptyList}
      importantForAccessibility={tab === name ? 'yes' : 'no-hide-descendants'}
      ListEmptyComponent={
        <Text style={s.empty}>
          {name === 'logs' ? 'No logs yet' : 'No messages yet'}
        </Text>
      }
    />
  );
  return (
    <SafeAreaView style={s.safe}>
      <StatusBar
        barStyle={dark ? 'light-content' : 'dark-content'}
        backgroundColor={c.bg}
      />
      <View style={s.container}>
        <Text style={s.heading}>{title}</Text>
        <View style={s.tabs}>
          {[4, 5].map((version) => (
            <Pressable
              key={version}
              testID={`mqtt-protocol-${version}`}
              accessibilityRole="radio"
              accessibilityState={{
                checked: protocolVersion === version,
                disabled:
                  Boolean(pending) ||
                  [
                    'connecting',
                    'connected',
                    'reconnecting',
                    'disconnecting',
                  ].includes(state),
              }}
              disabled={
                Boolean(pending) ||
                [
                  'connecting',
                  'connected',
                  'reconnecting',
                  'disconnecting',
                ].includes(state)
              }
              style={[s.tab, protocolVersion === version && s.tabSelected]}
              onPress={() => chooseProtocol(version)}
            >
              <Text style={s.tabText}>{protocolLabel(version)}</Text>
            </Pressable>
          ))}
        </View>
        <Text style={s.meta}>
          MQTT 3.1 (3) unsupported. Disconnect before switching.
        </Text>
        <Pressable
          testID="mqtt-profile"
          accessibilityRole="button"
          style={s.profile}
          onPress={() => setSheetOpen(true)}
        >
          <Text style={s.titleText}>
            {selected.label} · {selected.port}
          </Text>
          <Text style={s.meta}>
            {selected.transport.toUpperCase()} · {selected.auth} · {DEMO_TOPIC}
          </Text>
        </Pressable>
        <Text
          testID={error ? 'mqtt-state-error' : 'mqtt-state'}
          accessibilityLabel={error || `State: ${state}`}
          style={[s.text, error && s.error]}
        >
          State: {state}
          {error ? ` · ${error}` : ''}
        </Text>
        <View style={s.actions}>
          <Pressable
            testID="mqtt-test-roundtrip"
            accessibilityRole="button"
            disabled={state !== 'connected' || Boolean(pending)}
            style={[
              s.button,
              (state !== 'connected' || Boolean(pending)) && s.disabled,
            ]}
            onPress={() => runTest()}
          >
            <Text style={s.buttonText}>Test QoS 0/1/2</Text>
          </Pressable>
          <Pressable
            testID="mqtt-test-v5"
            accessibilityRole="button"
            disabled={
              protocolVersion !== 5 || state !== 'connected' || Boolean(pending)
            }
            style={[
              s.button,
              (protocolVersion !== 5 ||
                state !== 'connected' ||
                Boolean(pending)) &&
                s.disabled,
            ]}
            onPress={() => runTest(true)}
          >
            <Text style={s.buttonText}>Test v5 properties</Text>
          </Pressable>
          <Pressable
            testID="mqtt-connect"
            accessibilityRole="button"
            style={s.button}
            disabled={Boolean(pending) || Boolean(clientRef.current)}
            onPress={connect}
          >
            <Text style={s.buttonText}>Connect and subscribe</Text>
          </Pressable>
          <Pressable
            testID="mqtt-publish"
            accessibilityRole="button"
            style={[s.button, state !== 'connected' && s.disabled]}
            disabled={state !== 'connected' || Boolean(pending)}
            onPress={publish}
          >
            <Text
              style={[
                s.buttonText,
                state !== 'connected' && s.buttonTextDisabled,
              ]}
            >
              Publish timestamp
            </Text>
          </Pressable>
          <Pressable
            testID="mqtt-overflow"
            accessibilityRole="button"
            style={[s.button, state !== 'connected' && s.disabled]}
            disabled={state !== 'connected' || Boolean(pending)}
            onPress={burst}
          >
            <Text
              style={[
                s.buttonText,
                state !== 'connected' && s.buttonTextDisabled,
              ]}
            >
              JS block demo (3 sec)
            </Text>
          </Pressable>
          <Pressable
            testID="mqtt-disconnect"
            accessibilityRole="button"
            style={[s.button, !clientRef.current && s.disabled]}
            disabled={!clientRef.current || Boolean(pending)}
            onPress={disconnect}
          >
            <Text
              style={[s.buttonText, !clientRef.current && s.buttonTextDisabled]}
            >
              Disconnect
            </Text>
          </Pressable>
          <Pressable
            testID="mqtt-dispose"
            accessibilityRole="button"
            style={[s.button, (!clientRef.current || pending) && s.disabled]}
            disabled={!clientRef.current || Boolean(pending)}
            onPress={dispose}
          >
            <Text
              style={[
                s.buttonText,
                (!clientRef.current || pending) && s.buttonTextDisabled,
              ]}
            >
              Dispose client
            </Text>
          </Pressable>
        </View>
        {Boolean(testResult) && (
          <Text
            testID={`mqtt-test-result-${testResult.startsWith('PASS') ? 'pass' : testResult.startsWith('FAIL') ? 'fail' : 'running'}`}
            accessibilityLabel={testResult}
            style={s.text}
          >
            {testResult}
          </Text>
        )}
        <View style={s.tabs}>
          {['logs', 'messages'].map((name) => (
            <Pressable
              key={name}
              testID={`mqtt-tab-${name}`}
              accessibilityRole="tab"
              accessibilityState={{ selected: tab === name }}
              style={[s.tab, tab === name && s.tabSelected]}
              onPress={() => setTab(name)}
            >
              <Text style={s.tabText}>
                {name === 'logs'
                  ? `Logs (${logs.length}${logs.length === 500 ? '+' : ''})`
                  : `Messages (${messages.length}${messages.length === 500 ? '+' : ''})`}
              </Text>
            </Pressable>
          ))}
        </View>
        {renderList('logs', logs, renderLog)}
        {renderList('messages', messages, renderMessage)}
        {(tab === 'logs' ? logs.length : messages.length) === 500 && (
          <Text style={s.limitNotice}>
            History limit reached; oldest entries are removed.
          </Text>
        )}
        <Pressable
          testID={`mqtt-clear-${tab}`}
          accessibilityRole="button"
          accessibilityLabel={`Clear ${tab}`}
          onPress={() => (tab === 'logs' ? setLogs([]) : setMessages([]))}
        >
          <Text style={s.clear}>{`Clear ${tab}`}</Text>
        </Pressable>
      </View>
      <Modal
        visible={sheetOpen}
        transparent
        animationType="slide"
        onRequestClose={() => setSheetOpen(false)}
      >
        <View style={s.modal}>
          <Pressable style={s.scrim} onPress={() => setSheetOpen(false)} />
          <View style={s.sheet}>
            <View style={s.sheetHeader}>
              <Text style={s.sheetTitle}>Choose connection</Text>
              <Pressable
                testID="mqtt-profile-close"
                accessibilityRole="button"
                accessibilityLabel="Close connection chooser"
                onPress={() => setSheetOpen(false)}
              >
                <Text style={s.close}>Close</Text>
              </Pressable>
            </View>
            <FlatList
              data={availableProfiles}
              keyExtractor={(item) => String(item.port)}
              renderItem={({ item }) => (
                <Pressable
                  testID={`listener-${item.port}`}
                  accessibilityLabel={`listener-${item.port}`}
                  accessibilityRole="radio"
                  accessibilityState={{ selected: item.port === selected.port }}
                  style={[
                    s.option,
                    item.port === selected.port && s.optionSelected,
                  ]}
                  onPress={() => choose(item)}
                >
                  <Text style={s.titleText}>
                    {item.label} · {item.port}
                  </Text>
                  <Text style={s.meta}>
                    {item.transport.toUpperCase()} · {item.auth}
                    {item.expectedFailure ? ' · expected TLS failure' : ''}
                  </Text>
                </Pressable>
              )}
            />
          </View>
        </View>
      </Modal>
    </SafeAreaView>
  );
}
const styles = (c) =>
  StyleSheet.create({
    safe: { flex: 1, backgroundColor: c.bg },
    container: { flex: 1, padding: 16, gap: 10 },
    heading: { color: c.text, fontSize: 18, fontWeight: '700' },
    profile: {
      backgroundColor: c.surface,
      borderColor: c.border,
      borderWidth: 1,
      borderRadius: 10,
      padding: 12,
    },
    text: { color: c.text },
    error: { color: c.danger },
    titleText: { color: c.text, fontWeight: '600' },
    meta: { color: c.muted, marginTop: 4, fontSize: 12 },
    actions: {
      gap: 8,
      flexDirection: 'row',
      flexWrap: 'wrap',
      justifyContent: 'space-between',
    },
    button: {
      backgroundColor: c.primary,
      borderRadius: 8,
      padding: 12,
      alignItems: 'center',
      width: '48%',
    },
    buttonText: { color: '#fff', fontWeight: '600' },
    buttonTextDisabled: { color: c.disabledText },
    disabled: { backgroundColor: c.disabledButton },
    tabs: {
      flexDirection: 'row',
      borderBottomColor: c.border,
      borderBottomWidth: 1,
    },
    tab: { flex: 1, alignItems: 'center', padding: 11 },
    tabSelected: { borderBottomColor: c.primary, borderBottomWidth: 2 },
    tabText: { color: c.text, fontWeight: '600' },
    activeList: { flex: 1 },
    hiddenList: {
      position: 'absolute',
      width: 0,
      height: 0,
      opacity: 0,
    },
    list: { gap: 8, paddingVertical: 8 },
    emptyList: { flexGrow: 1, justifyContent: 'center' },
    empty: { color: c.muted, textAlign: 'center' },
    card: {
      backgroundColor: c.surface,
      borderColor: c.border,
      borderWidth: 1,
      borderRadius: 8,
      padding: 10,
    },
    clear: { color: c.primary, padding: 6 },
    limitNotice: { color: c.muted, fontSize: 12 },
    modal: { flex: 1, justifyContent: 'flex-end' },
    scrim: { ...StyleSheet.absoluteFillObject, backgroundColor: c.overlay },
    sheet: {
      maxHeight: '75%',
      backgroundColor: c.surface,
      borderTopLeftRadius: 18,
      borderTopRightRadius: 18,
      padding: 16,
    },
    sheetHeader: {
      flexDirection: 'row',
      alignItems: 'center',
      justifyContent: 'space-between',
    },
    sheetTitle: {
      color: c.text,
      fontSize: 18,
      fontWeight: '700',
      marginBottom: 8,
    },
    close: { color: c.primary, fontWeight: '600', padding: 6 },
    option: { padding: 12, borderBottomColor: c.border, borderBottomWidth: 1 },
    optionSelected: { backgroundColor: c.bg },
  });
