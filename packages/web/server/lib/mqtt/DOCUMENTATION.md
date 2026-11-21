# MQTT triggers module

Server-owned MQTT trigger runtime for OpenChamber. Reuses the scheduled-tasks
execution path (`runtime.triggerTask(projectID, taskID, 'mqtt', payload)`).

## Task shape

A task may carry an `mqtt` field instead of (or alongside) a `schedule`:

- `topic` (required): MQTT topic, may include `+` / `#` wildcards.
- `qos`: 0/1/2 (default 0).
- `broker`: broker URL (default `OPENCHAMBER_MQTT_URL` or `mqtt://localhost:1883`).
- `username` / `password`: optional auth.
- `payloadTemplate`: prompt template with `{{payload}}` (or `{{payload.key}}`) placeholder.
- `topicTemplate`: prompt template with `{{topic}}` placeholder.
- `handleRetained`: whether to fire on retained messages (default false).
- `minIntervalMs`: debounce between fires.
- `dedup`: deduplicate identical payloads within 60s (default true).

## Cross-instance

Deduplication is currently in-memory per process. For multi-instance setups
sharing a broker, a persistent claim (like scheduled occurrences) should be
added.

## Retained messages

By default retained messages received at (re)connection are ignored to avoid
spurious fires on restart. Set `handleRetained: true` to opt in.