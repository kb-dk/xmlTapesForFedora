package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by abr on 08-12-15.
 */
public class RedisTestSettings {

    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 16379;
    public static final int REDIS_DATABASE = 5;


    public static Index getIndex() {
        return new RedisIndex(REDIS_HOST, REDIS_PORT, REDIS_DATABASE, new JedisPoolConfig());
    }
}
