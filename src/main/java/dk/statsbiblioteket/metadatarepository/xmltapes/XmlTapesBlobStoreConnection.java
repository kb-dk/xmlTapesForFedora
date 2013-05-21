package dk.statsbiblioteket.metadatarepository.xmltapes;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.UnsupportedIdException;
import org.akubraproject.impl.AbstractBlobStoreConnection;
import org.akubraproject.impl.StreamManager;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/16/13
 * Time: 2:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class XmlTapesBlobStoreConnection extends AbstractBlobStoreConnection {

    private ZipArchive archive;

    protected XmlTapesBlobStoreConnection(BlobStore owner, StreamManager streamManager, ZipArchive archive) {
        super(owner, streamManager);
        this.archive = archive;
    }

    @Override
    public Blob getBlob(URI blobId, Map<String, String> hints) throws IOException, UnsupportedIdException, UnsupportedOperationException {
        if (blobId == null){
            throw new UnsupportedOperationException("Blobid was null, and we cannot create IDs");
        }
        return new XmlTapesBlob(this,blobId,archive);
    }

    @Override
    public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
        return archive.listIds(filterPrefix);
    }

    private Object acquireReadLock() {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    private void releaseReadLock(Object lock) {

    }

    @Override
    public void sync() throws IOException, UnsupportedOperationException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
