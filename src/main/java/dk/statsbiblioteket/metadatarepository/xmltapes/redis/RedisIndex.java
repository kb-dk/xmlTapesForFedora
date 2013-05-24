package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Index;
import redis.clients.jedis.Jedis;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/23/13
 * Time: 11:38 AM
 * To change this template use File | Settings | File Templates.
 */
public class RedisIndex implements Index {

    private Jedis jedis;

    public RedisIndex(String host, int port) {
        jedis = new Jedis(host,port);

    }

    @Override
    public List<Entry> getLocations(URI id) {
        List<String> files = jedis.lrange(id.toString(), 0, -1);
        ArrayList<Entry> result = new ArrayList<Entry>();
        for (String file : files) {
            result.add(Entry.deserialize(file));
        }
        return result;
    }

    @Override
    public void addLocation(URI id, Entry location) {
        jedis.lpush(id.toString(), location.serialize());
    }

    @Override
    public void remove(URI id) {
        jedis.del(id.toString());
    }

    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        Set<String> keys = jedis.keys(filterPrefix + "*");
        return new URIIterator(keys.iterator());

    }

    @Override
    public void clear() {
        jedis.flushDB();
    }
}
