package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.tarfiles.TapeArchiveImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

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

    @Test
    public void testRebuild() throws IOException, URISyntaxException {
        long tapeSize = 1024L * 1024;
        TapeArchiveImpl tapeArchive = new TapeArchiveImpl(getPrivateStoreId(), tapeSize, ".tar", "tape", "tempTape");
        tapeArchive.setIndex(index);
        index.clear();
        //Test that the index is clear
        tapeArchive.setRebuild(true);
        tapeArchive.init();
        Assert.assertTrue(tapeArchive.exist(URI.create("info:fedora/uuid:0cab5f49-b6ed-47d6-87d8-a257ba782246")));
        Assert.assertFalse(tapeArchive.exist(URI.create("uuid:0cab5f49-b6ed-47d6-87d8-a257ba782246")));
    }

    private static File getPrivateStoreId() throws URISyntaxException {
        File archiveFolder = new File(Thread.currentThread()
                                            .getContextClassLoader()
                                            .getResource("reindexTest/tape.tar")
                                            .toURI()).getParentFile();
        return archiveFolder;
    }


}
