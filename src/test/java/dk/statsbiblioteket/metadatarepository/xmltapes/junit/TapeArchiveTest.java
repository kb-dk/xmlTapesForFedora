package dk.statsbiblioteket.metadatarepository.xmltapes.junit;

import dk.statsbiblioteket.metadatarepository.xmltapes.TapeArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;
import dk.statsbiblioteket.metadatarepository.xmltapes.deferred2.AbstractDeferringArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.deferred2.Cache;
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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class TapeArchiveTest {

    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 6379;
    public static final int REDIS_DATABASE = 3;
    AbstractDeferringArchive archive;

    URI testFile1 = URI.create("testFile1");
    String contents = "testFile 1 is here now";
    private long tapeSize = 1024*1024;

    @Before
    public void setUp() throws Exception {

        URI store = getPrivateStoreId();
        File cachingDir = new File(new File(store), "cachingDir");
        cachingDir.mkdirs();
        File tempDir = new File(new File(store), "tempDir");
        tempDir.mkdirs();


        TapeArchive tapeArchive = new TapeArchive(store, tapeSize);



        archive = new Cache(cachingDir, tempDir);
        archive.setDelegate(tapeArchive);
        archive.setIndex(new RedisIndex(REDIS_HOST, REDIS_PORT, REDIS_DATABASE));
        archive.rebuild();
        OutputStream outputStream = archive.createNew(testFile1, 0);
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        writer.write(contents);
        writer.close();

    }


    private static URI getPrivateStoreId() throws URISyntaxException {
        URI archiveFolder = new File(Thread.currentThread().getContextClassLoader().getResource("archive/empty").toURI()).getParentFile().toURI();
        return archiveFolder;
    }


    @After
    public void clean() throws URISyntaxException, IOException, InterruptedException {
        //Thread.sleep(5000);
        archive.close();
        File archiveFolder = new File(getPrivateStoreId());
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

    }


}
