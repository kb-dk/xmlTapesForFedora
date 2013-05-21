package dk.statsbiblioteket.metadatarepository.xmltapes;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.DuplicateBlobException;
import org.akubraproject.MissingBlobException;
import org.akubraproject.impl.AbstractBlob;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/16/13
 * Time: 2:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class XmlTapesBlob extends AbstractBlob {

    private final ZipArchive archive;

    public XmlTapesBlob(BlobStoreConnection owner, URI id, ZipArchive archive) {
        super(owner, id);
        this.archive = archive;
    }

    @Override
    public InputStream openInputStream() throws IOException, MissingBlobException {
        InputStream result = archive.getInputStream(getId());
        if (result == null){
            throw new MissingBlobException(getId());
        }
        return result;
    }

    @Override
    public OutputStream openOutputStream(long estimatedSize, boolean overwrite) throws IOException, DuplicateBlobException {
        if (! overwrite && exists()){
            throw new DuplicateBlobException(getId());
        }
        return archive.createNew(getId(),estimatedSize);
    }

    @Override
    public long getSize() throws IOException, MissingBlobException {
        return archive.getSize(getId());
    }

    @Override
    public boolean exists() throws IOException {
        return archive.exist(getId());
    }

    @Override
    public void delete() throws IOException {
        archive.removeFromIndex(getId());
    }

    @Override
    public Blob moveTo(URI blobId, Map<String, String> hints) throws DuplicateBlobException, IOException, MissingBlobException, NullPointerException, IllegalArgumentException {
        ReadLock lock = archive.writeLock(blobId, this.getId());
        if (blobId == null){
            //what?
        }
        XmlTapesBlob newBlob = new XmlTapesBlob(getConnection(), blobId, archive);
        if (newBlob.exists()){
            throw new DuplicateBlobException(blobId,"New blob already exists");
        }
        if ( ! this.exists()){
            throw new MissingBlobException(this.getId());
        }


        OutputStream them = newBlob.openOutputStream(this.getSize(), true);
        InputStream my = openInputStream();
        IOUtils.copyLarge(my,them);
        them.close();
        my.close();
        this.delete();
        archive.releaseLock(lock);
        return newBlob;
    }
}
