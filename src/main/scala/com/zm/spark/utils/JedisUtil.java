package com.zm.spark.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.*;

public class JedisUtil {
    public static Set<String> smembers(Jedis jedis, String key) {
        if (jedis == null || key == null || key.length() == 0) {
            return null;
        }

        try {
            return jedis.smembers(key);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static Set<String> keys(Jedis jedis, String key) {
        if (jedis == null || key == null || key.length() == 0) {
            return null;
        }

        try {
            return jedis.keys(key);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static long incrBy(Jedis jedis, String key, long value) {
        if (jedis == null || key == null || key.length() == 0) {
            return 0;
        }

        try {
            return jedis.incrBy(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }

    public static boolean exists(Jedis jedis, String key) {
        if (jedis == null || key == null || key.length() == 0) {
            return false;
        }

        try {
            return jedis.exists(key);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    public static void set(Jedis jedis, String key, String value) {
        if (jedis == null || key == null || key.length() == 0 || value == null || value.length() == 0) {
            return;
        }

        try {
            jedis.set(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String get(Jedis jedis, String key) {
        if (jedis == null || key == null || key.length() == 0) {
            return null;
        }

        try {
            return jedis.get(key);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static Map<String, String> mget(Jedis jedis, List<String> keys) {
        try {
            if (jedis == null || keys == null || keys.size() == 0) {
                return null;
            }

            ArrayList<String> args = new ArrayList<String>();
            for (String key : keys) {
                args.add(key);
            }

            List<String> values = jedis.mget((String[]) args.toArray(new String[args.size()]));

            Map<String, String> data = new HashMap<String,String>();
            int i = 0;
            for (String value : values) {
                data.put(keys.get(i), value);
                i++;
            }

            return data;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void mset(Jedis jedis, Map<String, String> dataMap) {
        try {
            if (jedis == null || dataMap == null || dataMap.size() == 0) {
                return;
            }

            System.out.println(dataMap.size());

            ArrayList<String> args = new ArrayList<String>();
            for (Map.Entry<String, String> data : dataMap.entrySet()) {
                args.add(data.getKey());
                args.add(data.getValue());
            }

            jedis.mset((String[]) args.toArray(new String[args.size()]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sadd(Jedis jedis, String key, String value) {
        try {
            if (jedis == null || key == null || key.length() == 0 || value == null || value.length() == 0) {
                return;
            }

            jedis.sadd(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sadd(Jedis jedis, String key, Set<String> values) {
        try {
            if (jedis == null || key == null || key.length() == 0 || values == null || values.size() == 0) {
                return;
            }

            ArrayList<String> args = new ArrayList<String>();
            for (String value : values) {
                args.add(value);
            }

            jedis.sadd(key, (String[]) args.toArray(new String[args.size()]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void srem(Jedis jedis, String key, Set<String> values) {
        try {
            if (jedis == null || key == null || key.length() == 0 || values == null || values.size() == 0) {
                return;
            }

            ArrayList<String> args = new ArrayList<String>();
            for (String value : values) {
                args.add(value);
            }

            jedis.srem(key, (String[]) args.toArray(new String[args.size()]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Long llen(Jedis jedis, String key) {
        try {
            if (jedis == null || key == null || key.length() == 0) {
                return 0L;
            }
            return jedis.llen(key);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0L;
    }

    public static List<String> lrange(Jedis jedis, String key, long start, long end) {

        try {
            if (jedis == null || key == null || key.length() == 0) {
                return null;
            }
            return jedis.lrange(key, start, end);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void rpush(Jedis jedis, String key, List<String> values) {
        try {
            if (jedis == null || key == null || key.length() == 0 || values == null || values.size() == 0) {
                return;
            }

            ArrayList<String> args = new ArrayList<String>();
            for (String value : values) {
                args.add(value);
            }

            jedis.rpush(key, (String[]) args.toArray(new String[args.size()]));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return;
    }

    public static void lpush(Jedis jedis, String key, List<String> values) {
        try {
            if (jedis == null || key == null || key.length() == 0 || values == null || values.size() == 0) {
                return;
            }

            ArrayList<String> args = new ArrayList<String>();
            for (String value : values) {
                args.add(value);
            }

            jedis.lpush(key, (String[]) args.toArray(new String[args.size()]));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return;
    }

    public static String lpop(Jedis jedis, String key) {
        try {
            if (jedis == null || key == null || key.length() == 0) {
                return null;
            }

            return jedis.lpop(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void ltrim(Jedis jedis, String key, long start, long end) {
        try {
            if (jedis == null || key == null || key.length() == 0) {
                return;
            }

            jedis.ltrim(key, start, end);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return;
    }

    public static String hget(Jedis jedis, String key, String key2) {
        try {
            if (jedis == null || key == null || key.length() == 0) {
                return null;
            }
            return jedis.hget(key, key2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Set<Tuple> zrevrangeWithScores(Jedis jedis, String key, int min, int max) {
        if (jedis == null || key == null || key.length() == 0) {
            return null;
        }

        Set<Tuple> data = jedis.zrevrangeWithScores(key, min, max);
        return data;
    }

    public static String jtype(Jedis jedis, String key) {
        if (jedis == null || key == null || key.length() == 0) {
            return null;
        }

        String data = jedis.type(key);
        return data;
    }

}
