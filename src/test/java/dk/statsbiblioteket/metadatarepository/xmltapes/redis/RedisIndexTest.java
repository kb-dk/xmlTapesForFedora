package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;

import java.io.File;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.UUID;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/7/13
 * Time: 11:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class RedisIndexTest {
    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 6379;
    public static final int REDIS_DATABASE = 4;
    RedisIndex index;

    @Before
    public void setUp() throws Exception {
        index = new RedisIndex(REDIS_HOST, REDIS_PORT, REDIS_DATABASE, new JedisPoolConfig());
    }

    @After
    public void tearDown() throws Exception {
        index.clear();
    }

    @Ignore
    @Test
    public void testIterate() throws Exception {
        int objects = 10;

        HashSet<URI> ids = new HashSet<URI>();

        for (int i = 0; i < objects; i++) {
            addEntry();
        }
        Iterator<URI> idIterator = index.listIds(null);
        int count = 0;


        while (idIterator.hasNext()) {

            URI next = idIterator.next();
            count++;
            ids.add(next);
        }
        assertThat(objects, is(count));

        long iteratorKey = index.iterate(0);

        count = 0;
        long last = 0;
        long halfway = 0;
        int halfwaycount = 0;
        while (true){
            try {
                Record record = index.getRecord(iteratorKey);
                count++;
                ids.remove(URI.create(record.getId()));
                assertTrue(record.getTimestamp() >= last);
                last = record.getTimestamp();
                System.out.println(last);
                if (count == objects/2){
                    halfway = last;
                    halfwaycount++;
                    System.out.println("");
                }
                if (count > objects/2){
                    halfwaycount++;
                }

            } catch (Exception e){
                break;
            }
        }
        assertThat(objects, is(count));
        assertTrue(ids.isEmpty());


        System.out.println("");


        long iteratorKey2 = index.iterate(halfway);

        count = 0;
        last = 0;
        while (true){
            try {
                Record record = index.getRecord(iteratorKey2);
                count++;
                assertTrue(record.getTimestamp() >= last);
                last = record.getTimestamp();
                System.out.println(last);
            } catch (Exception e){
                break;
            }
        }
        assertThat(count, is(halfwaycount));
    }


    private void addEntry(){
        URI uri = URI.create(UUID.randomUUID().toString());
        index.addLocation(uri,getEntry(uri));
    }

    private Entry getEntry(URI uuid){
        return new Entry(new File("/"),0);
    }
}
