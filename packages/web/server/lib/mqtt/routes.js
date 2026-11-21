import { asNonEmptyString } from '../shared/guards.js';
import express from 'express';

export const registerMqttRoutes = (app, dependencies) => {
  const { mqttClient } = dependencies || {};

  app.post('/api/openchamber/mqtt/test', express.json({ limit: '16kb' }), async (req, res) => {
    const body = req.body && typeof req.body === 'object' ? req.body : {};
    const url = asNonEmptyString(body.url);
    if (!url) return res.status(400).json({ ok: false, error: 'url is required' });
    if (!mqttClient || typeof mqttClient.testConnection !== 'function') {
      return res.status(503).json({ ok: false, error: 'MQTT client not available' });
    }
    try {
      const result = await mqttClient.testConnection(url, {
        username: typeof body.username === 'string' ? body.username : undefined,
        password: typeof body.password === 'string' ? body.password : undefined,
      });
      return res.json(result);
    } catch (error) {
      return res.status(500).json({ ok: false, error: error?.message ?? String(error) });
    }
  });
};
