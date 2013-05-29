package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import redis.clients.jedis.Jedis;

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
    private final Jedis jedis;
    private final Iterator<String> buckets;
    private final String filterPrefix;
    private final int bufferSize = 1000;

    private String currentBucket;
    private int currentOffset = 0;

    private Iterator<String> currentSortedSet;

    private String currentValue = null;


    public RedisIterator(Jedis jedis, Set<String> buckets, String filterPrefix) {
        this.jedis = jedis;
        this.buckets = buckets.iterator();
        this.filterPrefix = filterPrefix;
        if (this.buckets.hasNext()){
            currentBucket = this.buckets.next();
        }
    }


    private boolean refreshSortedSet(){
        if (currentBucket == null){
            return false;
        }
        while (currentSortedSet == null || !currentSortedSet.hasNext()){
            currentSortedSet = jedis.zrange(currentBucket,currentOffset,currentOffset+bufferSize).iterator();
            currentOffset += bufferSize;
            if (!currentSortedSet.hasNext()){
                if (buckets.hasNext()){
                    currentBucket = buckets.next();
                    currentOffset = 0;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }
        return true;
    }


    @Override
    public boolean hasNext() {

        while (currentValue == null || !currentValue.startsWith(filterPrefix)){
            boolean doesStillHaveMore = refreshSortedSet();
            if (doesStillHaveMore){
                currentValue = currentSortedSet.next();
            } else {
                return doesStillHaveMore;
            }
        }
        return true;


    }

    @Override
    public URI next() {
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
