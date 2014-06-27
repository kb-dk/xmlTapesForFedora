package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/29/13
 * Time: 4:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class RedisIterator implements Iterator<URI> {

    private static final Logger log = LoggerFactory.getLogger(RedisIterator.class);

    private final JedisPool jedisPool;
    private final Iterator<String> buckets;
    private final String filterPrefix;
    private final int bufferSize = 1000;

    private String currentBucket;
    private int currentOffset = 0;

    private Iterator<String> currentSortedSet;

    private String currentValue = null;


    public RedisIterator(JedisPool jedis, Set<String> buckets, String filterPrefix) {
        log.debug("Initialising iterator for filterprefix {}",filterPrefix);
        this.jedisPool = jedis;
        this.buckets = buckets.iterator();
        this.filterPrefix = filterPrefix;
        if (this.buckets.hasNext()){
            currentBucket = this.buckets.next();
        }
    }

    /**
     * Get the next set of records from the current sorted set, or, if the current sorted set is empty, switch
     * to the next sortedsett and get a a set of records
     * @return
     */
    private boolean refreshSortedSet(){
        if (currentBucket == null){
            return false;
        }
        while (currentSortedSet == null || !currentSortedSet.hasNext()){
            log.debug("Time to read the next block from {} at offset ",currentBucket,currentOffset);
            Jedis jedis = jedisPool.getResource();
            try {
                currentSortedSet = jedis.zrange(currentBucket, currentOffset, currentOffset + bufferSize).iterator();
            } finally {
                jedisPool.returnResource(jedis);
            }
            currentOffset += bufferSize;
            if (!currentSortedSet.hasNext()){
                log.debug("bucket {} is empty, getting content from the next",currentBucket);
                if (buckets.hasNext()){
                    currentBucket = buckets.next();
                    currentOffset = 0;
                } else {
                    log.debug("We are not out of records");
                    return false;
                }
            } else {
                return true;
            }
        }
        return true;
    }


    @Override
    public synchronized boolean hasNext() {

        while (currentValue == null || !currentValue.startsWith(filterPrefix)){
            boolean doesStillHaveMore = refreshSortedSet();
            if (doesStillHaveMore){
                currentValue = currentSortedSet.next();
            } else {
                return false;
            }
        }
        return true;


    }

    @Override
    public synchronized URI next() {
        if (!hasNext() || currentValue == null){
            throw new NoSuchElementException();
        }

        URI result = URI.create(currentValue);
        currentValue = null;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
