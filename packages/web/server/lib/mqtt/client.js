// Lightweight MQTT client manager. One connection per broker URL, shared by all
// tasks that target it. The `mqtt` package is an optional dependency: when it
// is not installed the manager stays inert and logs a warning instead of
// crashing the server.
let mqttModule = null;
try {
  const mod = await import('mqtt');
  mqttModule = mod?.default || mod;
} catch (error) {
  mqttModule = null;
}

const connections = new Map();

const normalizeBrokerUrl = (value) => {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return null;
  if (/^[a-z]+:\/\//i.test(raw)) return raw;
  return `mqtt://${raw}`;
};

const createConnection = (brokerUrl, options = {}) => {
  if (!mqttModule) {
    console.warn('[mqtt] "mqtt" package is not installed; MQTT triggers are disabled.');
    return null;
  }
  const client = mqttModule.connect(brokerUrl, {
    clientId: options.clientId || `openchamber-${process.pid}-${Math.random().toString(16).slice(2)}`,
    username: options.username,
    password: options.password,
    reconnectPeriod: 2_000,
    connectTimeout: 10_000,
    clean: true,
    ...options.extra,
  });
  const state = {
    url: brokerUrl,
    client,
    subscriptions: new Map(), // topic -> Set<handler>
    connected: false,
    destroyed: false,
  };
  client.on('connect', () => {
    state.connected = true;
    console.log(`[mqtt] connected to ${brokerUrl}`);
    for (const topic of state.subscriptions.keys()) {
      try { client.subscribe(topic, { qos: 0 }); console.log(`[mqtt] subscribed to ${topic} on ${brokerUrl}`); } catch { /* ignore */ }
    }
  });
  client.on('reconnect', () => { state.connected = false; });
  client.on('close', () => { state.connected = false; console.log(`[mqtt] disconnected from ${brokerUrl}`); });
  client.on('error', (error) => {
    console.warn('[mqtt] connection error:', error?.message ?? error);
  });
  client.on('message', (topic, payloadBuffer, packet) => {
    const payload = payloadBuffer ? payloadBuffer.toString('utf8') : '';
    const retained = Boolean(packet?.retain);
    for (const [pattern, handlers] of state.subscriptions.entries()) {
      if (!topicMatches(pattern, topic)) continue;
      for (const handler of handlers) {
        try { handler({ topic, payload, retained, pattern }); }
        catch (error) { console.warn('[mqtt] handler error:', error?.message ?? error); }
      }
    }
  });
  connections.set(brokerUrl, state);
  return state;
};

// MQTT topic matching with + and # wildcards.
const topicMatches = (pattern, topic) => {
  if (pattern === topic) return true;
  const patternParts = String(pattern).split('/');
  const topicParts = String(topic).split('/');
  for (let i = 0; i < patternParts.length; i += 1) {
    const pp = patternParts[i];
    if (pp === '#') return true;
    if (i >= topicParts.length) return false;
    if (pp !== '+' && pp !== topicParts[i]) return false;
  }
  return patternParts.length === topicParts.length;
};

const getConnection = (brokerUrl, options) => {
  const url = normalizeBrokerUrl(brokerUrl);
  if (!url) return null;
  const existing = connections.get(url);
  if (existing && !existing.destroyed) return existing;
  return createConnection(url, options);
};

const subscribe = (brokerUrl, topic, handler, options = {}) => {
  const state = getConnection(brokerUrl, options);
  if (!state) return () => {};
  if (!state.subscriptions.has(topic)) {
    state.subscriptions.set(topic, new Set());
    if (state.connected) {
      try { state.client.subscribe(topic, { qos: options.qos ?? 0 }); } catch { /* ignore */ }
    }
  }
  state.subscriptions.get(topic).add(handler);
  return () => {
    const handlers = state.subscriptions.get(topic);
    if (!handlers) return;
    handlers.delete(handler);
    if (handlers.size === 0) {
      state.subscriptions.delete(topic);
      try { state.client.unsubscribe(topic); } catch { /* ignore */ }
    }
  };
};

const testConnection = (brokerUrl, options = {}) => {
  return new Promise((resolve) => {
    const url = normalizeBrokerUrl(brokerUrl);
    if (!url) return resolve({ ok: false, error: 'Invalid broker URL' });
    if (!mqttModule) return resolve({ ok: false, error: 'MQTT module not available' });
    let settled = false;
    const finish = (result) => {
      if (settled) return;
      settled = true;
      try { client.end(true); } catch { /* ignore */ }
      resolve(result);
    };
    const client = mqttModule.connect(url, {
      clientId: `openchamber-test-${process.pid}-${Math.random().toString(16).slice(2)}`,
      username: options.username,
      password: options.password,
      reconnectPeriod: 0,
      connectTimeout: 8_000,
      clean: true,
    });
    const timer = setTimeout(() => finish({ ok: false, error: 'Connection timed out' }), 9_000);
    client.on('connect', () => { clearTimeout(timer); finish({ ok: true }); });
    client.on('error', (error) => { clearTimeout(timer); finish({ ok: false, error: error?.message ?? String(error) }); });
    client.on('close', () => { if (!settled) { clearTimeout(timer); finish({ ok: false, error: 'Connection closed' }); } });
  });
};

const isAvailable = () => Boolean(mqttModule);

export {
  isAvailable,
  normalizeBrokerUrl,
  getConnection,
  subscribe,
  topicMatches,
  testConnection,
};
