const DEFAULT_BROKER = process.env.OPENCHAMBER_MQTT_URL || process.env.MQTT_URL || 'mqtt://localhost:1883';
const DEDUP_TTL_MS = 60_000;
const MIN_INTERVAL_DEFAULT = 0;

const topicKey = (projectID, taskID, topic, payload) => `${projectID}:${taskID}:${topic}:${payload}`;

const createMqttTriggerManager = (deps) => {
  const {
    mqttClient,
    scheduledTasksRuntime,
    logger = console,
  } = deps;

  const active = new Map(); // subscriptionKey -> { unsubscribe, tasks: Set }
  const recent = new Map(); // dedupKey -> timestamp
  const lastTriggeredAt = new Map(); // taskKey -> timestamp

  const pruneRecent = () => {
    const now = Date.now();
    for (const [key, ts] of recent.entries()) {
      if (now - ts > DEDUP_TTL_MS) recent.delete(key);
    }
  };

  const brokerForTask = (task) => task?.mqtt?.broker || DEFAULT_BROKER;
  const usernameForTask = (task) => task?.mqtt?.username || process.env.OPENCHAMBER_MQTT_USERNAME || process.env.MQTT_USERNAME;
  const passwordForTask = (task) => task?.mqtt?.password || process.env.OPENCHAMBER_MQTT_PASSWORD || process.env.MQTT_PASSWORD;

  const handle = (projectID, task, event) => {
    logger.log?.(`[mqtt-triggers] message received on ${event.topic} -> task ${task.id} (${task.name})`);
    const taskKey = `${projectID}:${task.id}`;
    const mqttCfg = task.mqtt || {};
    if (event.retained && mqttCfg.handleRetained !== true) {
      return;
    }
    const minInterval = typeof mqttCfg.minIntervalMs === 'number' ? mqttCfg.minIntervalMs : MIN_INTERVAL_DEFAULT;
    const last = lastTriggeredAt.get(taskKey) || 0;
    if (minInterval > 0 && Date.now() - last < minInterval) {
      return;
    }
    if (mqttCfg.dedup !== false) {
      pruneRecent();
      const key = topicKey(projectID, task.id, event.topic, event.payload);
      if (recent.has(key)) return;
      recent.set(key, Date.now());
    }
    lastTriggeredAt.set(taskKey, Date.now());
    try {
      scheduledTasksRuntime.triggerTask(projectID, task.id, 'mqtt', event.payload, event.topic)
        .catch((error) => logger.warn?.('[mqtt-triggers] trigger failed', { projectID, taskID: task.id, error: error?.message ?? error }));
    } catch (error) {
      logger.warn?.('[mqtt-triggers] trigger failed', { projectID, taskID: task.id, error: error?.message ?? error });
    }
  };

  const reconcile = () => {
    if (!mqttClient?.isAvailable?.()) {
      logger.warn?.('[mqtt-triggers] mqtt client unavailable; skipping reconcile');
      return;
    }
    // (reconcile runs frequently; only log concrete subscribe/unsubscribe changes below)
    const desired = new Map(); // brokerUrl -> Map(topic -> Set<{projectID,task}>)
    for (const { projectID, task } of scheduledTasksRuntime.listMqttTasks()) {
      const brokerUrl = brokerForTask(task);
      if (!desired.has(brokerUrl)) desired.set(brokerUrl, new Map());
      const byTopic = desired.get(brokerUrl);
      const topic = task.mqtt.topic;
      if (!byTopic.has(topic)) byTopic.set(topic, new Set());
      byTopic.get(topic).add({ projectID, task });
    }

    // Remove subscriptions no longer desired.
    for (const [subKey, entry] of Array.from(active.entries())) {
      const [brokerUrl, topic] = subKey.split('\u0000');
      const stillDesired = desired.get(brokerUrl)?.has(topic);
      if (!stillDesired) {
        logger.log?.(`[mqtt-triggers] unsubscribing ${topic} on ${brokerUrl}`);
        try { entry.unsubscribe(); } catch { /* ignore */ }
        active.delete(subKey);
      }
    }

    // Add new subscriptions.
    for (const [brokerUrl, byTopic] of desired.entries()) {
      for (const [topic, taskSet] of byTopic.entries()) {
        const subKey = `${brokerUrl}\u0000${topic}`;
        if (active.has(subKey)) {
          active.get(subKey).tasks = taskSet;
          continue;
        }
        logger.log?.(`[mqtt-triggers] subscribing to ${topic} on ${brokerUrl} (${taskSet.size} task(s))`);
        const unsubscribe = mqttClient.subscribe(
          brokerUrl,
          topic,
          (event) => {
            const entry = active.get(subKey);
            if (!entry) return;
            for (const { projectID, task } of entry.tasks) {
              handle(projectID, task, event);
            }
          },
          { qos: 0, username: usernameForTask([...taskSet][0].task), password: passwordForTask([...taskSet][0].task) },
        );
        active.set(subKey, { unsubscribe, tasks: taskSet });
      }
    }
  };

  const stop = () => {
    for (const entry of active.values()) {
      try { entry.unsubscribe(); } catch { /* ignore */ }
    }
    active.clear();
  };

  return { reconcile, stop };
};

export { createMqttTriggerManager };
