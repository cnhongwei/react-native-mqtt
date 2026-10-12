export const protocolLabel = (version) =>
  version === 4 ? 'MQTT 3.1.1 (4)' : 'MQTT 5.0 (5)';

export const demoConnectionOptions = (options = {}, version) => {
  const result = Object.fromEntries(
    Object.entries({ ...options, protocolVersion: version }).filter(
      ([, value]) => value !== undefined
    )
  );
  if (version === 4) {
    for (const key of [
      'cleanStart',
      'sessionExpiryIntervalSeconds',
      'receiveMaximum',
      'userProperties',
    ])
      delete result[key];
  } else {
    delete result.cleanSession;
  }
  return result;
};

// A successful publish ACK alone does not prove that the message came back.
export async function runProtocolDemo(
  client,
  {
    protocolVersion,
    properties = false,
    topic,
    encode,
    signal,
    timeoutMs = 15000,
    onProgress = () => {},
  }
) {
  if (
    ![4, 5].includes(protocolVersion) ||
    (properties && protocolVersion !== 5)
  ) {
    throw new Error(
      'MQTT 5 properties require protocolVersion 5; supported versions are 4 and 5'
    );
  }
  let subscription;
  let waiting;
  let activeTopic;
  const listener = client.onMessage((message) => {
    if (message.topic === activeTopic && waiting) waiting(message);
  });
  try {
    subscription = await client.subscribe(`${topic}/+`, { qos: 2 });
    for (const qos of properties ? [1] : [0, 1, 2]) {
      if (signal?.aborted) throw new Error('Test cancelled');
      // Late QoS 1 duplicates from a previous step must not fail the next step.
      activeTopic = `${topic}/${qos}`;
      const payload = `protocol-${protocolVersion}-qos-${qos}`;
      const bytes = encode(payload);
      const userProperties = [
        { key: 'demo', value: 'first' },
        { key: 'demo', value: 'second' },
      ];
      const options = properties
        ? { qos, userProperties, contentType: 'text/plain' }
        : { qos };
      let timer;
      let cancel;
      const deadline = new Promise((_, reject) => {
        cancel = () => reject(new Error('Test cancelled'));
        signal?.addEventListener('abort', cancel);
        timer = setTimeout(
          () => reject(new Error(`QoS ${qos}: round-trip timed out`)),
          timeoutMs
        );
      });
      const received = new Promise((resolve, reject) => {
        waiting = (message) => {
          try {
            if (
              message.retained ||
              message.qos !== qos ||
              message.bytes.length !== bytes.length ||
              !bytes.every((byte, index) => byte === message.bytes[index])
            ) {
              throw new Error(
                `QoS ${qos}: received payload/QoS/retain mismatch`
              );
            }
            if (
              properties &&
              (message.contentType !== 'text/plain' ||
                message.userProperties?.length !== userProperties.length ||
                !userProperties.every(
                  (entry, index) =>
                    message.userProperties[index].key === entry.key &&
                    message.userProperties[index].value === entry.value
                ))
            ) {
              throw new Error(
                'MQTT 5 properties missing, reordered or changed'
              );
            }
            resolve();
          } catch (error) {
            reject(error);
          }
        };
      });
      try {
        await Promise.race([
          deadline,
          Promise.all([
            received,
            Promise.resolve().then(() =>
              client.publishText(activeTopic, payload, options)
            ),
          ]),
        ]);
        onProgress(
          properties
            ? 'PASS: ordered duplicate User Properties + Content Type'
            : `PASS: QoS ${qos} publish + receive`
        );
      } finally {
        clearTimeout(timer);
        signal?.removeEventListener('abort', cancel);
        waiting = undefined;
      }
    }
  } finally {
    listener.remove();
    if (subscription) await subscription.unsubscribe();
  }
}
