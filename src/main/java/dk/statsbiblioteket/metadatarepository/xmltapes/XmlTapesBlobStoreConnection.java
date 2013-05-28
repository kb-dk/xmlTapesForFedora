package dk.statsbiblioteket.metadatarepository.xmltapes;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;
import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.UnsupportedIdException;
import org.akubraproject.impl.AbstractBlobStoreConnection;
import org.akubraproject.impl.StreamManager;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/16/13
 * Time: 2:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class XmlTapesBlobStoreConnection extends AbstractBlobStoreConnection {

    private Archive archive;



    protected XmlTapesBlobStoreConnection(BlobStore owner, StreamManager streamManager, Archive archive) {
        super(owner, streamManager);
        this.archive = archive;
    }

    @Override
    public Blob getBlob(URI blobId, Map<String, String> hints) throws IOException, UnsupportedIdException, UnsupportedOperationException {
        this.ensureOpen();
        if (blobId == null){
            throw new UnsupportedOperationException("Blobid was null, and we cannot create IDs");
        }
        return new XmlTapesBlob(this,blobId,archive,streamManager);
    }

    @Override
    public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
        this.ensureOpen();
        return archive.listIds(filterPrefix);
    }


    @Override
    public void sync() throws IOException, UnsupportedOperationException {
        this.ensureOpen();
    }
}
