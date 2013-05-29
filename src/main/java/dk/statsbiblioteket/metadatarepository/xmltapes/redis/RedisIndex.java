package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.RedisInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    public static final String BUCKETS = "buckets";

    private Jedis jedis;

    public RedisIndex(String host, int port, int database) {
        jedis = new Jedis(host, port);
        jedis.select(database);


    }

    @Override
    public Entry getLocation(URI id) {
        String file = jedis.get(id.toString());
        if (file == null || file.equals("nil")) {
            return null;
        }
        return Entry.deserialize(file);
    }

    @Override
    public void addLocation(URI id, Entry location) {
        String hashcode = getHash(id);
        Transaction trans = jedis.multi();

        trans.sadd(BUCKETS,hashcode);
        trans.zadd(hashcode, 1, id.toString());

        trans.set(id.toString(), location.serialize());
        trans.exec();
    }


    @Override
    public void remove(URI id) {

        String hashcode = getHash(id);
        Transaction trans = jedis.multi();
        trans.del(id.toString());
        trans.zrem(hashcode, id.toString());
        trans.exec();
        if (jedis.zcard(hashcode) == 0){
            jedis.del(hashcode);
            jedis.srem(BUCKETS,hashcode);
        }
    }

    private String getHash(URI id) {
        return id.hashCode()+"";
    }

    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        if (filterPrefix == null) {
            filterPrefix = "";
        }
        Set<String> buckets = jedis.smembers(BUCKETS);
        return new RedisIterator(jedis,buckets,filterPrefix);

    }

    @Override
    public boolean isIndexed(String tapename) {
        return jedis.sismember("tapes", tapename);
    }

    @Override
    public void setIndexed(String tapename) {
        jedis.sadd("tapes", tapename);
    }

    @Override
    public void clear() {
        jedis.flushDB();
    }
}
