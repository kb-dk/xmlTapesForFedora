package dk.statsbiblioteket.metadatarepository.xmltapes.testng;

import dk.statsbiblioteket.metadatarepository.xmltapes.akubra.XmlTapesBlobStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.cacheStore.CacheStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.junit.PostgresTestSettings;
import dk.statsbiblioteket.metadatarepository.xmltapes.junit.TestUtils;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
import dk.statsbiblioteket.metadatarepository.xmltapes.sqlindex.SQLIndex;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.Taper;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.TapingStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.tarfiles.TapeArchiveImpl;
import org.akubraproject.BlobStore;
import org.akubraproject.tck.TCKTestSuite;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterSuite;
import redis.clients.jedis.JedisPoolConfig;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/21/13
 * Time: 2:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class XmlTapesTestSuiteIT extends TCKTestSuite {


    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 6379;
    public static final int REDIS_DATABASE = 4;
    private static CacheStore archive;

    public XmlTapesTestSuiteIT() throws IOException, URISyntaxException {
        super(getPrivateStore(), getPrivateStoreId(), false, false);

    }

    private static File getStoreLocation() throws URISyntaxException {
        File archiveFolder = new File(Thread.currentThread().getContextClassLoader().getResource("archive/empty").toURI()).getParentFile();
        return archiveFolder;
    }

    private static URI getPrivateStoreId() throws URISyntaxException {
        URI name = URI.create("test:storageForTapes");
        return name;
    }

    public static BlobStore getPrivateStore() throws URISyntaxException, IOException {
        cleanBefore();

        URI privateStoreId = getPrivateStoreId();

        long tapeSize = 1024L * 1024;

        //create the cacheStore
        File cachingDir = TestUtils.mkdir(getStoreLocation(), "cachingDir");
        File tempDir = TestUtils.mkdir(getStoreLocation(), "tempDir");
        CacheStore cacheStore = new CacheStore(cachingDir, tempDir);
        //create the tapingStore
        File tapingDir = TestUtils.mkdir(getStoreLocation(), "tapingDir");
        TapingStore tapingStore = new TapingStore(tapingDir);
        tapingStore.setCache(cacheStore);
        tapingStore.setDelay(10);
        cacheStore.setDelegate(tapingStore);
        //create the TapeArchive
        TapeArchive tapeArchive = new TapeArchiveImpl(getStoreLocation(), tapeSize, ".tar", "tape", "tempTape");
        //RedisIndex redis = new RedisIndex(REDIS_HOST, REDIS_PORT, REDIS_DATABASE, new JedisPoolConfig());
        SQLIndex postgresIndex = PostgresTestSettings.getPostgreIndex();
        tapeArchive.setIndex(postgresIndex);
        //tapeArchive.setIndex(redis);
        tapingStore.setDelegate(tapeArchive);
        Taper taper = new Taper(tapingStore, cacheStore, tapeArchive);
        taper.setTapeDelay(1000);
        tapingStore.setTask(taper);
        archive = cacheStore;
        archive.init();


        XmlTapesBlobStore store = new XmlTapesBlobStore(getPrivateStoreId());
        store.setArchive(cacheStore);

        return store;
    }

    public static void cleanBefore() throws IOException, URISyntaxException {
        File archiveFolder = getStoreLocation();
        FileUtils.cleanDirectory(archiveFolder);
        FileUtils.touch(new File(archiveFolder, "empty"));
    }


    @AfterSuite
    public static void clean() throws IOException, URISyntaxException {

        if (archive != null){
            archive.close();
        }
        cleanBefore();

    }



    @Override
    protected URI getInvalidId() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected URI[] getAliases(URI uri) {
        return null;
    }


 /*   @Test( dependsOnGroups={ "post" })
    public void testFinal(){

        System.out.println("Final test");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }*/


}
