package dk.statsbiblioteket.metadatarepository.xmltapes;

import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Archive;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.AbstractBlobStore;
import org.akubraproject.impl.StreamManager;

import javax.transaction.Transaction;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/16/13
 * Time: 1:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class XmlTapesBlobStore extends AbstractBlobStore {

    private final StreamManager manager = new StreamManager();
    private Archive archive;

    /**
     * Create a new blob store.
     *
     * @param id the store's id
     */
    protected XmlTapesBlobStore(URI id) throws IOException {
        super(id);
    }

    @Override
    public BlobStoreConnection openConnection(Transaction tx, Map<String, String> hints) throws UnsupportedOperationException, IOException {
        if (tx != null){
            throw new UnsupportedOperationException("Blobstore is not transactional");
        }
        return new XmlTapesBlobStoreConnection(this,manager,archive);

    }



    public void setArchive(Archive archive) {
        this.archive = archive;
    }

    public Archive getArchive() {
        return archive;
    }
}
