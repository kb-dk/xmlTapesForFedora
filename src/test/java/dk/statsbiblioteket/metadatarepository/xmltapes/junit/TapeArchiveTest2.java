package dk.statsbiblioteket.metadatarepository.xmltapes.junit;

import dk.statsbiblioteket.metadatarepository.xmltapes.TapeArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class TapeArchiveTest2 {

    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 6379;
    public static final int REDIS_DATABASE = 3;
    Archive archive;

    URI testFile1 = URI.create("testFile1");
    URI testFile2 = URI.create("testFile2");
    URI testFile3 = URI.create("testFile3");
    String contents = "testFile 1 is here now";
    private long tapeSize = 1024*1024;
    RedisIndex index;
    
    
    @Before
    public void setUp() throws Exception {

        URI store = getPrivateStoreId();

        archive =  (new TapeArchive(store, tapeSize));

        index = new RedisIndex(REDIS_HOST, REDIS_PORT, REDIS_DATABASE);
        archive.setIndex(index);
        archive.rebuild();
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
        archive.rebuild();

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
