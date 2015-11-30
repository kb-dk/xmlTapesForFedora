package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;

import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Set;

/**
 *The Redis datamodel is as follows
 * A set called "Buckets"
 * This sets containts the hash values of the id all the objects in the repository. A proper hashing should indicate
 * that the number of unique members of the set is much smaller than the size of the repository
 * A number of sorted sets, named for the hash value they handle
 * These are the buckets, referenced above. The store the actual id of the objects
 *
 * The objects themselves are just ordinary keys from id to file,offset encoded.
 *

 */
public class RedisIndex implements Index {

    private static final Logger log = LoggerFactory.getLogger(RedisIndex.class);

    public static final String BUCKETS = "buckets";
    private static final int INDEX_LEVELS = 4;
    private static final int ITERATOR_LIFETIME = 60 * 60;
    public static final String TAPES_SET = "tapes";
    public static final String PLACEHOLDER = "placeholder";
    private final JedisPool pool;


    public RedisIndex(String host, int port, int database, JedisPoolConfig jedisPoolConfig) {

        pool = new JedisPool(jedisPoolConfig,host, port,0,null,database);

        log.info("Redis database {} initialised on {}:{}",new Object[]{database,host,port});
    }

    @Override
    public Entry getLocation(URI id) {
        Jedis jedis = pool.getResource();
        try {
            String file = jedis.get(id.toString());
            if (file == null || file.equals("nil")) {
                log.debug("Requested id {} but was not found in index", id.toString());
                return null;
            }
            return Entry.deserialize(file);
        } finally {
            pool.returnResource(jedis);
        }
    }

    @Override
    public void addLocation(URI id, Entry location) {
        Jedis jedis = pool.getResource();
        try {
            String hashcode = getHash(id);
            Transaction trans = jedis.multi();
            trans.sadd(BUCKETS, hashcode);
            trans.zadd(hashcode, 1, id.toString());
            trans.set(id.toString(), location.serialize());
            trans.exec();
        } finally {
            pool.returnResource(jedis);
        }

    }


    @Override
    public void remove(URI id) {
        Jedis jedis = pool.getResource();
        try {
            String hashcode = getHash(id);
            Transaction trans = jedis.multi();
            trans.del(id.toString());
            trans.zrem(hashcode, id.toString());
            trans.exec();
            if (jedis.zcard(hashcode) == 0) {
                jedis.del(hashcode);
                jedis.srem(BUCKETS, hashcode);
            }
        } finally {
            pool.returnResource(jedis);
        }

    }

    private static String getHash(URI id) {
        String result;
        try {
            result = Bytes.toHex(MessageDigest.getInstance("MD5").digest(id.toString().getBytes()));
        } catch (NoSuchAlgorithmException e) {
            throw new Error("MD5 not known");
        }
        return result.substring(0, INDEX_LEVELS);
    }

    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        log.debug("Listing all ids for prefix '{}'",filterPrefix);
        if (filterPrefix == null) {
            filterPrefix = "";
        }
        Jedis jedis = pool.getResource();
        try {
            Set<String> buckets = jedis.smembers(BUCKETS);
            return new RedisIterator(pool, buckets, filterPrefix);
        } finally {
            pool.returnResource(jedis);
        }
    }

    @Override
    public boolean isIndexed(String tapename) {
        Jedis jedis = pool.getResource();
        try {
            return jedis.sismember(TAPES_SET, tapename);
        } finally {
            pool.returnResource(jedis);
        }
    }

    @Override
    public void setIndexed(String tapename) {
        Jedis jedis = pool.getResource();
        try {
            jedis.sadd(TAPES_SET, tapename);
        } finally {
            pool.returnResource(jedis);
        }

    }

    @Override
    public void clear() {
        Jedis jedis = pool.getResource();
        try {
            jedis.flushDB();
        } finally {
            pool.returnResource(jedis);
        }
    }

}
