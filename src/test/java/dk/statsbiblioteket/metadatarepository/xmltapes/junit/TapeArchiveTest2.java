package dk.statsbiblioteket.metadatarepository.xmltapes.junit;

import dk.statsbiblioteket.metadatarepository.xmltapes.cache.CacheForDeferringTaper;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
import dk.statsbiblioteket.metadatarepository.xmltapes.taper.DeferringTaper;
import dk.statsbiblioteket.metadatarepository.xmltapes.tarfiles.TapeArchiveImpl;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;


public class TapeArchiveTest2 {

    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 6379;
    public static final int REDIS_DATABASE = 3;


    URI testFile1 = URI.create("testFile1");
    URI testFile2 = URI.create("testFile2");
    URI testFile3 = URI.create("testFile3");
    String contents = "testFile 1 is here now";
    private long tapeSize = 1024*1024;
    RedisIndex index;

    CacheForDeferringTaper archive;
    private TapeArchive underlyingTapeArchive;


    @Before
    public void setUp() throws Exception {

        URI store = getPrivateStoreId();
        File tapingDir = new File(new File(store), "tapingDir");
        tapingDir.mkdirs();
        File cachingDir = new File(new File(store), "cachingDir");
        cachingDir.mkdirs();
        File tempDir = new File(new File(store), "tempDir");
        tempDir.mkdirs();


        archive = new CacheForDeferringTaper(cachingDir, tempDir);
        underlyingTapeArchive = new TapeArchiveImpl(store, tapeSize, ".tar", "tape", "tempTape");
        DeferringTaper taper = new DeferringTaper(tapingDir);
        taper.setDelay(10);
        taper.setTapeDelay(1000);

        archive.setDelegate(taper);
        taper.setDelegate(underlyingTapeArchive);
        taper.setParent(archive);

        index = new RedisIndex(REDIS_HOST, REDIS_PORT, REDIS_DATABASE);
        underlyingTapeArchive.setIndex(index);
        underlyingTapeArchive.rebuild();
        archive.init();
        OutputStream outputStream = archive.createNew(testFile1, 0);
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        writer.write(contents);
        writer.close();


        outputStream = archive.createNew(testFile2, 0);
        writer = new OutputStreamWriter(outputStream);
        writer.write(contents);
        writer.close();


        outputStream = archive.createNew(testFile3, 0);
        writer = new OutputStreamWriter(outputStream);
        writer.write(contents);
        writer.close();

    }


    private static URI getPrivateStoreId() throws URISyntaxException {
        URI archiveFolder = new File(Thread.currentThread().getContextClassLoader().getResource("archive/empty").toURI()).getParentFile().toURI();
        return archiveFolder;
    }


    @After
    public void clean() throws URISyntaxException, IOException{
        archive.close();
        File archiveFolder = new File(getPrivateStoreId());
        FileUtils.cleanDirectory(archiveFolder);
        FileUtils.touch(new File(archiveFolder, "empty"));
    }


    @Test
    public void testGetInputStream() throws Exception {

        int offsetBefore = 0;


        Iterator<URI> uriIterator = index.listIds("");
        while (uriIterator.hasNext()) {
            URI next = uriIterator.next();
            Entry entry = index.getLocation(next);
            offsetBefore += entry.getOffset();
        }
        underlyingTapeArchive.rebuild();

        int offsetAfter = 0;

        uriIterator = index.listIds("");
        while (uriIterator.hasNext()) {
            URI next = uriIterator.next();
            Entry entry = index.getLocation(next);
            offsetAfter += entry.getOffset();
        }
        assertEquals(offsetBefore,offsetAfter);



    }



}
