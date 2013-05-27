package dk.statsbiblioteket.metadatarepository.xmltapes;

import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Archive;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

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

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/21/13
 * Time: 9:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class ZipArchiveTest {

    Archive archive;

    URI testFile1 = URI.create("testFile1");
    String contents = "testFile 1 is here now";
    private long tapeSize = 1024*1024;

    @Before
    public void setUp() throws Exception {

        URI store = getPrivateStoreId();

        archive = new ZipArchive(store, tapeSize);
        archive.setIndex(new RedisIndex("localhost",6379));
        archive.init();
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
    public void clean() throws URISyntaxException, IOException{
        File archiveFolder = new File(getPrivateStoreId());
        FileUtils.cleanDirectory(archiveFolder);
        FileUtils.touch(new File(archiveFolder, "empty"));
    }


    @org.junit.Test
    public void testGetInputStream() throws Exception {

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(archive.getInputStream(testFile1)));
        String contentsFromArchive = bufferedReader.readLine();
        assertThat(contentsFromArchive, is(contents));
        bufferedReader.close();

    }

    @org.junit.Test
    public void testExist() throws Exception {
        assertTrue(archive.exist(testFile1));

    }

    @org.junit.Test
    public void testGetSize() throws Exception {
        assertThat(archive.getSize(testFile1),is((long)contents.length()));
    }

    @org.junit.Test
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
