package dk.statsbiblioteket.metadatarepository.xmltapes.migrator.redis.postgres;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.migrator.IndexMigrator;
import dk.statsbiblioteket.metadatarepository.xmltapes.migrator.IntegrationTestImmediateReporter;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
import dk.statsbiblioteket.metadatarepository.xmltapes.sqlindex.SQLIndex;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPoolConfig;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by abr on 14-12-15.
 */
@Listeners(IntegrationTestImmediateReporter.class)
public class RedisToPostgresIT {

    @Test
    public void testRedisToPostgresMigration() throws Exception {


        String tape1 = "tape1";
        String tape2 = "tape2";
        String tape3 = "tape3";
        String tape4 = "tape4";

        //List the entries in From
        URI blob1ID = URI.create("blob1");
        Entry entry1 = new Entry(new File(tape2), 0);

        URI blob2ID = URI.create("blob2");
        Entry entry2 = new Entry(new File(tape2), 10045);

        URI blob3ID = URI.create("blob3");
        Entry entry3 = new Entry(new File(tape3), 0);

        Index postgres = new SQLIndex("org.postgresql.Driver", "jdbc:postgresql://localhost:15432/tapes", "docker", "docker");
        postgres.clear();
        Index redis = new RedisIndex("localhost",16379,5,new JedisPoolConfig());
        redis.clear();

        redis.addLocation(blob1ID,entry1);
        redis.addLocation(blob2ID,entry2);
        redis.addLocation(blob3ID,entry3);
        redis.setIndexed(tape1);
        redis.setIndexed(tape2);
        redis.setIndexed(tape3);

        IndexMigrator migrator = new IndexMigrator(redis, postgres);

        migrator.migrate();

        assertEquals(postgres.getLocation(blob1ID),entry1);
        assertEquals(postgres.getLocation(blob2ID),entry2);
        assertEquals(postgres.getLocation(blob3ID),entry3);

        assertEquals(postgres.isIndexed(tape1),true);
        assertEquals(postgres.isIndexed(tape2),true);
        assertEquals(postgres.isIndexed(tape3),true);
        assertEquals(postgres.isIndexed(tape4), false);

        Set<URI> keys = new HashSet<>(Arrays.asList(blob1ID,blob2ID,blob3ID));
        Iterator<URI> uriIterator = postgres.listIds(null);
        while (uriIterator.hasNext()) {
            URI next = uriIterator.next();
            assertTrue(keys.remove(next));
        }
        assertTrue(keys.isEmpty());

        Set<String> tapes = new HashSet<>(Arrays.asList(tape1,tape2,tape3));
        Iterator<String> tapeIterator = postgres.listIndexedTapes();
        while (tapeIterator.hasNext()) {
            String next = tapeIterator.next();
            assertTrue(tapes.remove(next));
        }
        assertTrue(tapes.isEmpty());
    }
}
