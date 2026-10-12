import assert from 'node:assert/strict';
import { ScenarioRunner, ControllableTransport, VirtualClock } from '../test/scenarios/virtual-clock.mjs';

const clock = new VirtualClock();
const transport = new ControllableTransport(clock);
const events = [];
transport.setMessageHandler((message) => events.push(['message', message.id]));
transport.setDisconnectHandler((reason) => events.push(['disconnect', reason]));

const timeoutId = clock.setTimeout(() => events.push(['timer', clock.now()]), 25);
clock.setTimeout(() => transport.emitDisconnect('late'), 10);
transport.emitMessage({ id: 'first' });
transport.emitMessage({ id: 'duplicate' });
clock.advanceBy(10);
clock.clearTimeout(timeoutId);
clock.advanceBy(20);

assert.deepEqual(events, [
  ['message', 'first'],
  ['message', 'duplicate'],
  ['disconnect', 'late'],
]);
assert.equal(clock.now(), 30);
assert.equal(transport.connectAttempts, 0);

const runnerClock = new VirtualClock();
const runnerTransport = new ControllableTransport(runnerClock);
const runnerEvents = [];
runnerTransport.setMessageHandler((message) => runnerEvents.push(message.id));
runnerTransport.setDisconnectHandler((reason) => runnerEvents.push(reason));
new ScenarioRunner({ clock: runnerClock, transport: runnerTransport }).run([
  { type: 'connect' },
  { type: 'connect' },
  { type: 'complete-connect', index: 1, value: 'second' },
  { type: 'complete-connect', index: 0, value: 'first' },
  { type: 'delayed-message', ms: 20, value: { id: 'late' } },
  { type: 'delayed-message', ms: 10, value: { id: 'early' } },
  { type: 'message', value: { id: 'repeat' } },
  { type: 'message', value: { id: 'repeat' } },
  { type: 'delayed-disconnect', ms: 15, value: 'out-of-order' },
]);
assert.equal(runnerTransport.connectAttempts, 2);
runnerClock.advanceBy(10);
assert.deepEqual(runnerEvents, ['repeat', 'repeat', 'early']);
runnerClock.advanceBy(10);
assert.deepEqual(runnerEvents, ['repeat', 'repeat', 'early', 'out-of-order', 'late']);

const operationRunner = new ScenarioRunner();
operationRunner.run([
  { type: 'subscribe', filter: 'devices/+', options: { qos: 1 } },
  { type: 'subscribe', filter: 'devices/#', options: { qos: 0 } },
  { type: 'publish', topic: 'devices/a', payload: Uint8Array.from([1, 2]) },
  { type: 'transport-disconnect' },
]);
assert.deepEqual(operationRunner.transport.subscribeAttempts, [
  { filter: 'devices/+', options: { qos: 1 } },
  { filter: 'devices/#', options: { qos: 0 } },
]);
assert.deepEqual(operationRunner.transport.publishAttempts, [
  { topic: 'devices/a', payload: Uint8Array.from([1, 2]), options: undefined },
]);
assert.equal(operationRunner.transport.disconnectAttempts, 1);
operationRunner.run([
  { type: 'complete-subscribe', index: 1, value: { grantedQos: 0 } },
  { type: 'complete-subscribe', index: 0, value: { grantedQos: 1 } },
  { type: 'fail-publish', message: 'publish callback failure' },
  { type: 'complete-disconnect', value: undefined },
]);
await operationRunner.operations[0];
await operationRunner.operations[1];
await assert.rejects(operationRunner.operations[2], /publish callback failure/);
await operationRunner.operations[3];

const failedRunner = new ScenarioRunner();
failedRunner.run([
  { type: 'connect' },
  { type: 'fail-connect', message: 'immediate send failure' },
]);
await assert.rejects(failedRunner.operations[0], /immediate send failure/);

const failedConnect = transport.connect();
assert.equal(transport.connectAttempts, 1);
transport.failNextConnect(new Error('immediate send failure'));
await assert.rejects(failedConnect, /immediate send failure/);

console.log('Scenario primitives smoke passed.');
