package dk.statsbiblioteket.metadatarepository.xmltapes;

import de.schlichtherle.truezip.file.TVFS;
import de.schlichtherle.truezip.fs.FsSyncException;
import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
public class XmlTapesBlobStoreTest {

    BlobStoreConnection connection;

    @Before
    public void setUp() throws Exception {
        connection = getPrivateStore().openConnection(null,null);

    }


    private static URI getPrivateStoreId() throws URISyntaxException {
        URI archiveFolder = new File(Thread.currentThread().getContextClassLoader().getResource("archive/empty").toURI()).getParentFile().toURI();
        return archiveFolder;
    }


    public BlobStore getPrivateStore() throws URISyntaxException, IOException {
        clean();
        return new XmlTapesBlobStore(getPrivateStoreId());
    }
    @After
    public void clean() throws IOException, URISyntaxException {
        TVFS.umount();
        File archiveFolder = new File(getPrivateStoreId());
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
