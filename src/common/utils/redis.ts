const redis = require('async-redis');
import * as dotenv from "dotenv";
import * as _ from 'lodash'
dotenv.config();

export const getRedisOpts = (url) => {
  const urlObject = new URL(url);
  const result = {
    host: urlObject.hostname,
    port: +urlObject.port || 6379,
    username: urlObject.username,
    password: urlObject.password,
    db: urlObject.pathname
      ? +urlObject.pathname.replace('/', '')
      : 0
  };
  return result;
};

class RedisService {
  private _client: any;
  private _ttl: number;
  constructor(url: any, ttl?: number) {
    if (!url) {
      throw new Error("Config redis url doesn't empty.");
    }
    this._ttl = ttl
    this._client = redis.createClient({
      url,
      retry_strategy(options) {
        if (options.total_retry_time > 1000 * 60 * 60) {
          return new Error("Retry time exhausted");
        }
        return 1000;
      }
    });
  }

  private _scanAndDel = async (pattern: string, cursor = '0') => {
    const keys = await this._client.scan(
      cursor,
      'MATCH',
      pattern,
      'COUNT',
      '5000'
    );

    cursor = keys[0];
    for (const key of keys[1]) {
      this._client.del(key);
    }

    return cursor === '0'
      ? true
      : await this._scanAndDel(pattern, cursor);
  };

  async get(key: string): Promise<any | null | undefined> {
    if (!key) return null;
    const result = await this._client.get(key);
    if (!result) {
      return null;
    }
    try {
      return JSON.parse(result);
    } catch (e) {
      return result;
    }
  }

  async ttl(key: string): Promise<number | null | undefined> {
    if (!key) return null
    const result = await this._client.ttl(key)
    return result
  }

  async set(key: string, data: any, ttl?: number): Promise<boolean> {
    if (ttl > 0 || this._ttl > 0) {
      return await this._client.set(key, JSON.stringify(data), "EX", ttl || this._ttl);
    }
    return await this._client.set(key, JSON.stringify(data));
  }

  async del(key: string): Promise<boolean> {
    await this._client.del(key);
    return true;
  }

  async delByPattern(pattern: string): Promise<boolean> {
    return this._scanAndDel(pattern);
  }

  async keys(pattern: string): Promise<any | null | undefined> {
    if (!pattern) return null;
    const result = await this._client.keys(pattern);
    if (!result.length) {
      return null;
    }
    return result
  }

  async hincrby(hash: string, field: string, increment: number): Promise<boolean> {
    return await this._client.hincrby(hash, field, increment)
  }

  async hgetall(key: string): Promise<any | null | undefined> {
    if (!key) return null
    const result = await this._client.hgetall(key)
    return result
  }

  async hdel(hash: string, ...fields: string[]): Promise<boolean> {
    return await this._client.hdel(hash, ...fields)
  }
}


const DEFAULT_CACHE_TTL = 15 * 60;

export const redisSession = new RedisService(process.env.SESSION_REDIS_URL);
export const redisCache = new RedisService(process.env.CACHE_REDIS_URL, DEFAULT_CACHE_TTL);

