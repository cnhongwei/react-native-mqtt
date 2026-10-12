export type PublicListener = {
  port: number;
  transport: 'tcp' | 'tls' | 'ws' | 'wss';
  tls: boolean;
  auth: string;
  certificate: string;
  path?: string;
  expected?: 'tls-error';
};

// Public smoke matrix defined by the repository OpenSpec. Do not use it for
// performance tests or sensitive payloads.
export const publicListeners: PublicListener[] = [
  {
    port: 1883,
    transport: 'tcp',
    tls: false,
    auth: 'anonymous',
    certificate: 'none',
  },
  {
    port: 1884,
    transport: 'tcp',
    tls: false,
    auth: 'username/password',
    certificate: 'none',
  },
  {
    port: 8883,
    transport: 'tls',
    tls: true,
    auth: 'anonymous',
    certificate: 'Mosquitto CA',
  },
  {
    port: 8884,
    transport: 'tls',
    tls: true,
    auth: 'client certificate (mTLS)',
    certificate: 'Mosquitto CA + client certificate',
  },
  {
    port: 8885,
    transport: 'tls',
    tls: true,
    auth: 'username/password',
    certificate: 'server certificate validation required',
  },
  {
    port: 8886,
    transport: 'tls',
    tls: true,
    auth: 'anonymous',
    certificate: 'system/Let’s Encrypt CA',
  },
  {
    port: 8887,
    transport: 'tls',
    tls: true,
    auth: 'anonymous',
    certificate: 'expected expired-certificate failure',
    expected: 'tls-error',
  },
  {
    port: 8080,
    transport: 'ws',
    tls: false,
    auth: 'anonymous',
    certificate: 'none',
    path: '/mqtt',
  },
  {
    port: 8081,
    transport: 'wss',
    tls: true,
    auth: 'anonymous',
    certificate: 'system/Let’s Encrypt CA',
    path: '/mqtt',
  },
  {
    port: 8090,
    transport: 'ws',
    tls: false,
    auth: 'username/password',
    certificate: 'none',
    path: '/mqtt',
  },
  {
    port: 8091,
    transport: 'wss',
    tls: true,
    auth: 'username/password',
    certificate: 'system/Let’s Encrypt CA',
    path: '/mqtt',
  },
];
