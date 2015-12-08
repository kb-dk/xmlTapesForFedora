package dk.statsbiblioteket.metadatarepository.xmltapes.abstracts;

import dk.statsbiblioteket.metadatarepository.xmltapes.TestNamesListener;
import dk.statsbiblioteket.metadatarepository.xmltapes.TestUtils;
import dk.statsbiblioteket.metadatarepository.xmltapes.cacheStore.CacheStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.Taper;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.TapingStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.tarfiles.TapeArchiveImpl;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;


@Listeners(TestNamesListener.class)
public abstract class AbstractTapeArchive2IT {

    URI testFile1 = URI.create("testFile1");
    URI testFile2 = URI.create("testFile2");
    URI testFile3 = URI.create("testFile3");
    String contents = "testFile 1 is here now";
    Index index;

    CacheStore archive;
    private TapeArchive underlyingTapeArchive;


    @BeforeMethod
    public void setUp() throws Exception {
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
        index = getIndex();
        tapeArchive.setIndex(index);
        tapingStore.setDelegate(tapeArchive);
        Taper taper = new Taper(tapingStore, cacheStore, tapeArchive);
        taper.setTapeDelay(1000);
        tapingStore.setTask(taper);
        archive = cacheStore;
        archive.init();
        underlyingTapeArchive = tapeArchive;


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

    protected abstract Index getIndex();


    private static File getPrivateStoreId() throws URISyntaxException {
        File archiveFolder = new File(Thread.currentThread().getContextClassLoader().getResource("archive/empty").toURI()).getParentFile();
        return archiveFolder;
    }


    @AfterMethod
    public void clean() throws URISyntaxException, IOException{
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
