import os from 'node:os';
import { performance } from 'node:perf_hooks';

class BoundedInbox {
  constructor({ maxMessages, maxBytes, batchMessages, batchBytes }) {
    this.maxMessages = maxMessages;
    this.maxBytes = maxBytes;
    this.batchMessages = batchMessages;
    this.batchBytes = batchBytes;
    this.queue = [];
    this.inFlight = undefined;
    this.accepted = 0;
    this.dropped = 0;
    this.maxObservedMessages = 0;
    this.maxObservedBytes = 0;
  }

  enqueue(bytes) {
    const inFlightMessages = this.inFlight?.length ?? 0;
    const inFlightBytes = this.inFlight?.reduce((total, item) => total + item, 0) ?? 0;
    const queuedBytes = this.queue.reduce((total, item) => total + item, 0);
    if (
      bytes > this.maxBytes ||
      this.queue.length + inFlightMessages >= this.maxMessages ||
      queuedBytes + inFlightBytes + bytes > this.maxBytes
    ) {
      this.dropped += 1;
      return false;
    }
    this.queue.push(bytes);
    this.accepted += 1;
    this.observe();
    return true;
  }

  startBatch() {
    if (this.inFlight || this.queue.length === 0) return undefined;
    const batch = [];
    let bytes = 0;
    while (this.queue.length > 0 && batch.length < this.batchMessages) {
      const next = this.queue[0];
      if (batch.length > 0 && bytes + next > this.batchBytes) break;
      batch.push(this.queue.shift());
      bytes += next;
    }
    this.inFlight = batch;
    this.observe();
    return batch;
  }

  acknowledge() {
    this.inFlight = undefined;
    this.observe();
  }

  observe() {
    const messages = this.queue.length + (this.inFlight?.length ?? 0);
    const bytes = this.queue.reduce((total, item) => total + item, 0) +
      (this.inFlight?.reduce((total, item) => total + item, 0) ?? 0);
    this.maxObservedMessages = Math.max(this.maxObservedMessages, messages);
    this.maxObservedBytes = Math.max(this.maxObservedBytes, bytes);
  }
}

const percentile = (values, fraction) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))] ?? 0;
};

const runCase = ({ name, payloadBytes, messages, pauseMs }) => {
  const inbox = new BoundedInbox({
    maxMessages: 1024,
    maxBytes: 8 * 1024 * 1024,
    batchMessages: 64,
    batchBytes: 256 * 1024,
  });
  const enqueueDurations = [];
  const before = process.memoryUsage().rss;
  const started = performance.now();
  inbox.enqueue(payloadBytes);
  inbox.startBatch();
  const pauseStarted = performance.now();
  while (performance.now() - pauseStarted < pauseMs) {
    // Deliberately occupy the JS turn while native-style delivery continues.
    Math.imul(31, 17);
  }
  for (let index = 1; index < messages; index += 1) {
    const enqueueStarted = performance.now();
    inbox.enqueue(payloadBytes);
    enqueueDurations.push(performance.now() - enqueueStarted);
  }
  inbox.acknowledge();
  while (inbox.startBatch()) inbox.acknowledge();
  const elapsedMs = performance.now() - started;
  const after = process.memoryUsage().rss;
  return {
    name,
    payloadBytes,
    messages,
    pauseMs,
    elapsedMs: Number(elapsedMs.toFixed(3)),
    acceptedMessages: inbox.accepted,
    throughputAcceptedMessagesPerSecond: Number((inbox.accepted / (elapsedMs / 1000)).toFixed(2)),
    enqueueP95Ms: Number(percentile(enqueueDurations, 0.95).toFixed(4)),
    droppedMessages: inbox.dropped,
    maxObservedMessages: inbox.maxObservedMessages,
    maxObservedBytes: inbox.maxObservedBytes,
    rssDeltaBytes: after - before,
  };
};

const results = [
  runCase({ name: 'paused-small-packets', payloadBytes: 128, messages: 100_000, pauseMs: 250 }),
  runCase({ name: 'paused-large-packets', payloadBytes: 512 * 1024, messages: 1_000, pauseMs: 250 }),
];

console.log(JSON.stringify({
  node: process.version,
  platform: `${process.platform}/${process.arch}`,
  cpuCount: os.cpus().length,
  cpuModel: os.cpus()[0]?.model,
  loadAverage: os.loadavg(),
  results,
  limits: { maxMessages: 1024, maxBytes: 8 * 1024 * 1024 },
  limitation: 'Host JS bounded-inbox benchmark; not native bridge/device throughput or memory acceptance.',
}, null, 2));
