package dk.statsbiblioteket.metadatarepository.xmltapes.abstracts;

import dk.statsbiblioteket.metadatarepository.xmltapes.IntegrationTestImmediateReporter;
import dk.statsbiblioteket.metadatarepository.xmltapes.TestUtils;
import dk.statsbiblioteket.metadatarepository.xmltapes.cacheStore.CacheStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.AkubraCompatibleArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.Taper;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.TapingStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.tarfiles.TapeArchiveImpl;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Listeners(IntegrationTestImmediateReporter.class)
public abstract class AbstractTapeArchiveIT {

    AkubraCompatibleArchive archive;
    Index index;

    URI testFile1 = URI.create("testFile1");
    String contents = "testFile 1 is here now";

    @BeforeMethod
    public void setUp() throws Exception {
        //TODO this setup is duplicated for all the test classes. Simplify
        clean();
        File store = getPrivateStoreId();
        long tapeSize = 1024L * 1024;

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
        index = getIndex();
        tapeArchive.setIndex(index);
        tapingStore.setDelegate(tapeArchive);

        Taper taper = new Taper(tapingStore, cacheStore, tapeArchive);
        taper.setTapeDelay(1000);
        tapingStore.setTask(taper);
        archive = cacheStore;
        archive.init();

        OutputStream outputStream = archive.createNew(testFile1, 0);
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        writer.write(contents);
        writer.close();

    }

    protected abstract Index getIndex();

    private  File getPrivateStoreId() throws URISyntaxException {
        File archiveFolder = new File(Thread.currentThread().getContextClassLoader().getResource("archive/empty").toURI()).getParentFile();
        archiveFolder = new File(archiveFolder,getClass().getSimpleName());
        archiveFolder.mkdirs();
        return archiveFolder;
    }


    @AfterMethod
    public void clean() throws URISyntaxException, IOException, InterruptedException {
        //Thread.sleep(5000);
        if (archive != null) {
            archive.close();
        }
        if(index != null) {
            index.clear();
        }
        File archiveFolder = getPrivateStoreId();
        FileUtils.cleanDirectory(archiveFolder);
        FileUtils.touch(new File(archiveFolder, "empty"));
    }


    @Test
    public void testGetInputStream() throws Exception {

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(archive.getInputStream(testFile1)));
        String contentsFromArchive = bufferedReader.readLine();
        assertThat(contentsFromArchive, is(contents));
        bufferedReader.close();

    }

    @Test
    public void testExist() throws Exception {
        assertTrue(archive.exist(testFile1));

    }

    @Test
    public void testGetSize() throws Exception {
        assertThat(archive.getSize(testFile1),is((long)contents.length()));
    }

    @Test
    public void testCreateNew() throws Exception {

        assertTrue(archive.exist(testFile1));

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(archive.getInputStream(testFile1)));
        String contentsFromArchive = bufferedReader.readLine();
        assertThat(contentsFromArchive, is(contents));
        bufferedReader.close();

        OutputStream outputStream = archive.createNew(testFile1, 0);
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        String contents2 = "testFile 2 is here now";
        writer.write(contents2);
        writer.close();

        bufferedReader = new BufferedReader(new InputStreamReader(archive.getInputStream(testFile1)));
        contentsFromArchive = bufferedReader.readLine();
        assertThat(contentsFromArchive, is(contents2));
        bufferedReader.close();
        Thread.sleep(5000);

    }


}
