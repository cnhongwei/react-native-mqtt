import { useEffect, useState } from 'react';
import { NativeModules, Pressable, Text, View } from 'react-native';

const { PahoProbe } = NativeModules as {
  PahoProbe: {
    diagnose(): Promise<void>;
    connect(port: number, transport: string): Promise<void>;
    disconnect(): Promise<void>;
  };
};

export default function App() {
  const [port, setPort] = useState(1883);
  const [transport, setTransport] = useState<'tcp' | 'ws'>('tcp');
  const [state, setState] = useState('idle');
  const [error, setError] = useState('');
  const run = async (action: () => Promise<void>) => {
    setError('');
    try {
      await action();
    } catch (value) {
      setError(String(value));
      setState('error');
    }
  };
  useEffect(() => {
    run(async () => {
      await PahoProbe.diagnose();
      await PahoProbe.connect(1883, 'tcp');
      setState('tcp-connected');
      await PahoProbe.disconnect();
      await PahoProbe.connect(8080, 'ws');
      setState('ws-connected');
      await PahoProbe.disconnect();
      setState('disconnected');
    });
  }, []);
  return (
    <View style={{ flex: 1, gap: 16, padding: 32, justifyContent: 'center' }}>
      <Text>RN Paho A/B Probe</Text>
      <Text>Selected: {port}</Text>
      <Text>State: {state}</Text>
      <Pressable
        onPress={() => {
          setPort(1883);
          setTransport('tcp');
        }}
      >
        <Text>1883 · TCP</Text>
      </Pressable>
      <Pressable
        onPress={() => {
          setPort(8080);
          setTransport('ws');
        }}
      >
        <Text>8080 · WS</Text>
      </Pressable>
      <Pressable
        onPress={() =>
          run(async () => {
            await PahoProbe.connect(port, transport);
            setState('connected');
          })
        }
      >
        <Text>Connect and subscribe</Text>
      </Pressable>
      <Pressable
        onPress={() =>
          run(async () => {
            await PahoProbe.disconnect();
            setState('disconnected');
          })
        }
      >
        <Text>Disconnect</Text>
      </Pressable>
      {!!error && <Text>{error}</Text>}
    </View>
  );
}
