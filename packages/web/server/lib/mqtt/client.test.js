import { describe, expect, it } from 'vitest';
import { topicMatches } from './client.js';

describe('mqtt topicMatches', () => {
  it('matches exact topics', () => {
    expect(topicMatches('a/b', 'a/b')).toBe(true);
    expect(topicMatches('a/b', 'a/c')).toBe(false);
  });
  it('matches + wildcard', () => {
    expect(topicMatches('a/+/c', 'a/b/c')).toBe(true);
    expect(topicMatches('a/+/c', 'a/b/d')).toBe(false);
    expect(topicMatches('a/+/c', 'a/b/c/d')).toBe(false);
  });
  it('matches # wildcard', () => {
    expect(topicMatches('a/#', 'a/b/c')).toBe(true);
    expect(topicMatches('#', 'anything/goes')).toBe(true);
    expect(topicMatches('a/#', 'a')).toBe(true);
  });
});