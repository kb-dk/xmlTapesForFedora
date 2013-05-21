package dk.statsbiblioteket.metadatarepository.xmltapes;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

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
        URI uri = Thread.currentThread().getContextClassLoader().getResource("empty.zip").toURI();
        connection = new XmlTapesBlobStore(uri).openConnection(null, null);

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
        String storedContents = IOUtils.toString(blob1.openInputStream());
        assertThat(storedContents, is(contents));


    }
}
