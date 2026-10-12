import { createMqttClient } from '../client';
import { MqttError } from '../errors';

describe('client factory', () => {
  test('is synchronous and allocates no native session', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    expect(client.state).toBe('idle');
    expect((await client.getSnapshot()).lastConnAck).toBeNull();
    await client.dispose();
    expect(client.state).toBe('closed');
  });

  test('rejects invalid factory input synchronously', () => {
    expect(() => createMqttClient({ host: '', port: 1883 })).toThrow(MqttError);
    expect(() =>
      createMqttClient({ host: 'broker.example', port: 1883, path: '/mqtt' })
    ).toThrow(MqttError);
  });

  test('reports native absence at operation time', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    await expect(client.connect()).rejects.toMatchObject({
      code: 'ERR_NATIVE_UNAVAILABLE',
    });
  });
});
