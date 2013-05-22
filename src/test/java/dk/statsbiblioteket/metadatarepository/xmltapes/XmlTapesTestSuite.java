package dk.statsbiblioteket.metadatarepository.xmltapes;

import org.akubraproject.BlobStore;
import org.akubraproject.tck.TCKTestSuite;

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
public class XmlTapesTestSuite extends TCKTestSuite {

    public XmlTapesTestSuite(BlobStore store, URI storeId, boolean isTransactional, boolean isIdGenSupp) throws IOException {
        super(store, storeId, isTransactional, isIdGenSupp);
    }

    protected XmlTapesTestSuite() throws URISyntaxException, IOException {
        this(new XmlTapesBlobStore(
                Thread.currentThread().getContextClassLoader().getResource("empty.tar").toURI())
                , Thread.currentThread().getContextClassLoader().getResource("empty.tar").toURI()
                , false
                , false);
    }

    @Override
    protected URI getInvalidId() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected URI[] getAliases(URI uri) {
        return null;
    }


}
