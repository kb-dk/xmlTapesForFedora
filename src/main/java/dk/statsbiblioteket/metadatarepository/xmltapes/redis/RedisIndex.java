package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;

import java.net.URI;
import java.util.Iterator;
import java.util.Set;

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
    public Entry getLocation(URI id) {
        String file = jedis.get(id.toString());
        if (file == null || file.equals("nil")){
            return null;
        }
        return Entry.deserialize(file);
    }

    @Override
    public void addLocation(URI id, Entry location) {
        jedis.set(id.toString(), location.serialize());
    }

    @Override
    public void remove(URI id) {
        jedis.del(id.toString());
    }

    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        //This does not perfor
        Set<String> keys = jedis.keys(filterPrefix + "*");
        return new URIIterator(keys.iterator());

    }

    @Override
    public boolean isIndexed(String tapename) {
        return jedis.exists(tapename);
    }

    @Override
    public void setIndexed(String tapename) {
        jedis.set(tapename,System.currentTimeMillis()+"");
    }

    @Override
    public void clear() {
        jedis.flushDB();
    }
}
