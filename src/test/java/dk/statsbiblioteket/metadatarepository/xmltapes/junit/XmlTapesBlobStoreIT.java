package dk.statsbiblioteket.metadatarepository.xmltapes.junit;

import dk.statsbiblioteket.metadatarepository.xmltapes.akubra.XmlTapesBlobStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.cacheStore.CacheStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.AkubraCompatibleArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
import dk.statsbiblioteket.metadatarepository.xmltapes.sqlindex.SQLIndex;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.Taper;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.TapingStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.tarfiles.TapeArchiveImpl;
import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/21/13
 * Time: 2:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class XmlTapesBlobStoreIT {

    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 6379;
    public static final int REDIS_DATABASE = 5;
    BlobStoreConnection connection;
    private AkubraCompatibleArchive archive;

    @Before
    public void setUp() throws Exception {
        connection = getPrivateStore().openConnection(null,null);

    }


    private static File getPrivateStoreId() throws URISyntaxException {
        File archiveFolder = new File(Thread.currentThread().getContextClassLoader().getResource("archive/empty").toURI()).getParentFile();
        return archiveFolder;
    }


    public BlobStore getPrivateStore() throws URISyntaxException, IOException {
        clean();

        File store = getPrivateStoreId();
        long tapeSize = 1024L * 1024;

        //Create the blobstore
        XmlTapesBlobStore xmlTapesBlobStore = new XmlTapesBlobStore(URI.create("test:tapestorage"));

        //create the cacheStore
        File cachingDir = TestUtils.mkdir(store, "cachingDir");
        File tempDir = TestUtils.mkdir(store, "tempDir");
        CacheStore cacheStore = new CacheStore(cachingDir, tempDir);

        //create the tapingStore
        File tapingDir = TestUtils.mkdir(store, "tapingDir");
        TapingStore tapingStore = new TapingStore(tapingDir);
        tapingStore.setCache(cacheStore);
        tapingStore.setDelay(10);
        cacheStore.setDelegate(tapingStore);

        //create the TapeArchive
        TapeArchive tapeArchive = new TapeArchiveImpl(store, tapeSize, ".tar", "tape", "tempTape");
        //RedisIndex redis = new RedisIndex(REDIS_HOST, REDIS_PORT, REDIS_DATABASE, new JedisPoolConfig());
        SQLIndex postgresIndex = PostgresTestSettings.getPostgreIndex();
        tapeArchive.setIndex(postgresIndex);
        tapingStore.setDelegate(tapeArchive);

        Taper taper = new Taper(tapingStore, cacheStore, tapeArchive);
        taper.setTapeDelay(1000);
        tapingStore.setTask(taper);


        archive = cacheStore;
        archive.init();
        xmlTapesBlobStore.setArchive(archive);
        return xmlTapesBlobStore;
    }

    @After
    public void clean() throws IOException, URISyntaxException {
        if (connection != null){
            connection.close();
        }
        if (archive != null){
            archive.close();
        }
        File archiveFolder = getPrivateStoreId();
        FileUtils.cleanDirectory(archiveFolder);
        FileUtils.touch(new File(archiveFolder, "empty"));

    }



    @Test
    public void testPutAndGetBlob() throws Exception {
        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("Blob1");
        String contents = IOUtils.toString(resourceAsStream);

        //reopen
        resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("Blob1");


        Blob blob1 = connection.getBlob(new URI("Blob1"), null);
        assertTrue(!blob1.exists());

        OutputStream outputStream = blob1.openOutputStream(contents.length(), true);
        IOUtils.copyLarge(resourceAsStream,outputStream);
        resourceAsStream.close();
        outputStream.close();
        InputStream inputStream = blob1.openInputStream();
        String storedContents = IOUtils.toString(inputStream);
        inputStream.close();
        assertThat(storedContents, is(contents));




    }
}
