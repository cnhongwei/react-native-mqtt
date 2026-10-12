/**
 * Deterministic test primitives for native-session scenario tests.
 * The clock is monotonic and never consults wall-clock time.
 */
export class VirtualClock {
  #now = 0;
  #nextId = 0;
  #timers = new Map();

  now() {
    return this.#now;
  }

  setTimeout(callback, delayMs) {
    if (!Number.isInteger(delayMs) || delayMs < 0) {
      throw new RangeError('virtual timer delay must be a non-negative integer');
    }
    const id = ++this.#nextId;
    this.#timers.set(id, { at: this.#now + delayMs, callback });
    return id;
  }

  clearTimeout(id) {
    this.#timers.delete(id);
  }

  advanceBy(deltaMs) {
    if (!Number.isInteger(deltaMs) || deltaMs < 0) {
      throw new RangeError('virtual clock delta must be a non-negative integer');
    }
    return this.advanceTo(this.#now + deltaMs);
  }

  advanceTo(targetMs) {
    if (!Number.isInteger(targetMs) || targetMs < this.#now) {
      throw new RangeError('virtual clock cannot move backwards');
    }
    let executed = 0;
    while (true) {
      const due = [...this.#timers.entries()]
        .filter(([, timer]) => timer.at <= targetMs)
        .sort((left, right) => left[1].at - right[1].at || left[0] - right[0]);
      if (due.length === 0) break;
      const [id, timer] = due[0];
      this.#timers.delete(id);
      this.#now = timer.at;
      timer.callback();
      executed += 1;
    }
    this.#now = targetMs;
    return executed;
  }

  runAll() {
    let executed = 0;
    while (this.#timers.size > 0) {
      const next = Math.min(...[...this.#timers.values()].map((timer) => timer.at));
      executed += this.advanceTo(next);
    }
    return executed;
  }
}

const deferred = () => {
  let resolve;
  let reject;
  const promise = new Promise((resolveValue, rejectValue) => {
    resolve = resolveValue;
    reject = rejectValue;
  });
  return { promise, resolve, reject };
};

/** A transport fake whose callback ordering is controlled by the test. */
export class ControllableTransport {
  constructor(clock) {
    this.clock = clock;
    this.connectAttempts = 0;
    this.subscribeAttempts = [];
    this.publishAttempts = [];
    this.disconnectAttempts = 0;
    this.connectDeferred = [];
    this.subscribeDeferred = [];
    this.publishDeferred = [];
    this.disconnectDeferred = [];
    this.messageHandler = undefined;
    this.disconnectHandler = undefined;
  }

  connect() {
    this.connectAttempts += 1;
    const operation = deferred();
    this.connectDeferred.push(operation);
    return operation.promise;
  }

  subscribe(filter, options) {
    this.subscribeAttempts.push({ filter, options });
    const operation = deferred();
    this.subscribeDeferred.push(operation);
    return operation.promise;
  }

  publish(topic, payload, options) {
    this.publishAttempts.push({ topic, payload, options });
    const operation = deferred();
    this.publishDeferred.push(operation);
    return operation.promise;
  }

  disconnect() {
    this.disconnectAttempts += 1;
    const operation = deferred();
    this.disconnectDeferred.push(operation);
    return operation.promise;
  }

  setMessageHandler(handler) {
    this.messageHandler = handler;
  }

  setDisconnectHandler(handler) {
    this.disconnectHandler = handler;
  }

  emitMessage(message) {
    this.messageHandler?.(message);
  }

  emitMessageAfter(delayMs, message) {
    return this.clock.setTimeout(() => this.emitMessage(message), delayMs);
  }

  emitDisconnect(reason) {
    this.disconnectHandler?.(reason);
  }

  emitDisconnectAfter(delayMs, reason) {
    return this.clock.setTimeout(() => this.emitDisconnect(reason), delayMs);
  }

  completeNextConnect(value) {
    const operation = this.connectDeferred.shift();
    if (!operation) throw new Error('no pending connect attempt');
    operation.resolve(value);
  }

  completeConnectAt(index, value) {
    const operation = this.connectDeferred.splice(index, 1)[0];
    if (!operation) throw new Error(`no pending connect attempt at index ${index}`);
    operation.resolve(value);
  }

  failNextConnect(error) {
    const operation = this.connectDeferred.shift();
    if (!operation) throw new Error('no pending connect attempt');
    operation.reject(error);
  }

  completeNextSubscribe(value) {
    const operation = this.subscribeDeferred.shift();
    if (!operation) throw new Error('no pending subscribe attempt');
    operation.resolve(value);
  }

  completeSubscribeAt(index, value) {
    const operation = this.subscribeDeferred.splice(index, 1)[0];
    if (!operation) throw new Error(`no pending subscribe attempt at index ${index}`);
    operation.resolve(value);
  }

  failNextSubscribe(error) {
    const operation = this.subscribeDeferred.shift();
    if (!operation) throw new Error('no pending subscribe attempt');
    operation.reject(error);
  }

  failSubscribeAt(index, error) {
    const operation = this.subscribeDeferred.splice(index, 1)[0];
    if (!operation) throw new Error(`no pending subscribe attempt at index ${index}`);
    operation.reject(error);
  }

  completeNextPublish(value) {
    const operation = this.publishDeferred.shift();
    if (!operation) throw new Error('no pending publish attempt');
    operation.resolve(value);
  }

  completePublishAt(index, value) {
    const operation = this.publishDeferred.splice(index, 1)[0];
    if (!operation) throw new Error(`no pending publish attempt at index ${index}`);
    operation.resolve(value);
  }

  failNextPublish(error) {
    const operation = this.publishDeferred.shift();
    if (!operation) throw new Error('no pending publish attempt');
    operation.reject(error);
  }

  failPublishAt(index, error) {
    const operation = this.publishDeferred.splice(index, 1)[0];
    if (!operation) throw new Error(`no pending publish attempt at index ${index}`);
    operation.reject(error);
  }

  completeNextDisconnect(value) {
    const operation = this.disconnectDeferred.shift();
    if (!operation) throw new Error('no pending disconnect attempt');
    operation.resolve(value);
  }

  completeDisconnectAt(index, value) {
    const operation = this.disconnectDeferred.splice(index, 1)[0];
    if (!operation) throw new Error(`no pending disconnect attempt at index ${index}`);
    operation.resolve(value);
  }
}

/** Runs transport-only actions against one virtual clock and fake instance. */
export class ScenarioRunner {
  constructor({ clock = new VirtualClock(), transport = new ControllableTransport(clock) } = {}) {
    this.clock = clock;
    this.transport = transport;
    this.operations = [];
  }

  trackOperation(operation) {
    // Keep the original promise for assertions while preventing an action-only
    // scenario from creating an unhandled rejection before it inspects it.
    operation.catch(() => undefined);
    this.operations.push(operation);
    return operation;
  }

  run(actions) {
    for (const action of actions) {
      switch (action.type) {
        case 'connect':
          this.trackOperation(this.transport.connect());
          break;
        case 'complete-connect':
          this.transport.completeConnectAt(action.index ?? 0, action.value);
          break;
        case 'fail-connect':
          this.transport.failNextConnect(new Error(action.message ?? 'connect failed'));
          break;
        case 'delay':
          this.clock.advanceBy(action.ms);
          break;
        case 'message':
          this.transport.emitMessage(action.value);
          break;
        case 'delayed-message':
          this.transport.emitMessageAfter(action.ms, action.value);
          break;
        case 'subscribe':
          this.trackOperation(this.transport.subscribe(action.filter, action.options));
          break;
        case 'complete-subscribe':
          if (action.index === undefined) this.transport.completeNextSubscribe(action.value);
          else this.transport.completeSubscribeAt(action.index, action.value);
          break;
        case 'fail-subscribe':
          if (action.index === undefined) this.transport.failNextSubscribe(new Error(action.message ?? 'subscribe failed'));
          else this.transport.failSubscribeAt(action.index, new Error(action.message ?? 'subscribe failed'));
          break;
        case 'publish':
          this.trackOperation(this.transport.publish(action.topic, action.payload, action.options));
          break;
        case 'complete-publish':
          if (action.index === undefined) this.transport.completeNextPublish(action.value);
          else this.transport.completePublishAt(action.index, action.value);
          break;
        case 'fail-publish':
          if (action.index === undefined) this.transport.failNextPublish(new Error(action.message ?? 'publish failed'));
          else this.transport.failPublishAt(action.index, new Error(action.message ?? 'publish failed'));
          break;
        case 'transport-disconnect':
          this.trackOperation(this.transport.disconnect());
          break;
        case 'complete-disconnect':
          if (action.index === undefined) this.transport.completeNextDisconnect(action.value);
          else this.transport.completeDisconnectAt(action.index, action.value);
          break;
        case 'disconnect':
          this.transport.emitDisconnect(action.value);
          break;
        case 'delayed-disconnect':
          this.transport.emitDisconnectAfter(action.ms, action.value);
          break;
        default:
          throw new Error(`unknown scenario action: ${action.type}`);
      }
    }
    return this;
  }
}
