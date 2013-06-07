package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Record;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
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
    private static final int INDEX_LEVELS = 4;
    private static final int ITERATOR_LIFETIME = 60 * 60;

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
    public void addLocation(URI id, Entry location, long timestamp) {
        String hashcode = getHash(id);
        Transaction trans = jedis.multi();

        trans.sadd(BUCKETS,hashcode);
        trans.zadd(hashcode, timestamp, id.toString());

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
        return Hasher.getHash(id.toString()).substring(0,INDEX_LEVELS);
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







    @Override
    public long iterate(long startTimestamp) {
        String key = new String(String.valueOf(new Random(startTimestamp).nextLong()));
        Set<String> buckets = jedis.smembers(BUCKETS);

        jedis.zadd(key,0,"placeholder");
        jedis.expire(key, ITERATOR_LIFETIME);
        //could be somewhat big

/*  //This kills the redis instance
        jedis.zunionstore(key,
                new ArrayList<String>(buckets).toArray(new String[buckets.size()]));
*/

        jedis.zremrangeByScore(key,"-inf","("+startTimestamp);
        return Long.valueOf(key);
    }

    @Override
    public Record getRecord(long iteratorKey) {
        List<Record> records = getRecords(iteratorKey, 1);
        if (records.isEmpty()){
            throw new NoSuchElementException();
        }
        return records.get(0);
    }

    private Record toRecord(String element, double score) {
        return new Record(element, (long) score);
    }

    @Override
    public List<Record> getRecords(long iteratorKey, int amount) {
        String key = iteratorKey+"";
        Transaction multi = jedis.multi();

        Response<Set<Tuple>> value = multi.zrangeWithScores(key, 0, amount-1);
        multi.zremrangeByRank(key,0,amount-1);
        multi.exec();
        Set<Tuple> recordsFound = value.get();
        List<Record> result = new ArrayList<Record>(recordsFound.size());
        for (Tuple tuple : recordsFound) {
            result.add(toRecord(tuple.getElement(),tuple.getScore()));
        }
        if (recordsFound.size() < amount){
            jedis.del(key);
        }
        return result;
    }


}
