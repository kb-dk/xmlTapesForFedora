package dk.statsbiblioteket.metadatarepository.xmltapes.deferred2;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/20/13
 * Time: 11:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class Cache extends AbstractDeferringArchive{

    private final File tempDir;


    public Cache(Archive taper, File cacheDir, File tempDir) {
        super(taper, cacheDir);
        this.tempDir = tempDir;
    }

    private File getTempFile(URI id) throws IOException {
        File tempfile = File.createTempFile(id.toString(), "temp", tempDir);
        tempfile.deleteOnExit();
        return tempfile;
    }




    @Override
    public OutputStream createNew(URI id, long estimatedSize) throws IOException {
        File tempFile = getTempFile(id);
        return new CacheOutputStream(tempFile, getDeferredFile(id));

    }


    @Override
    public void remove(URI id) throws IOException {
        //HERE WE NEED TO SET THAT THE BLOB IS DEAD
        File cacheFile = getDeferredFile(id);
        cacheFile.delete();
        cacheFile.createNewFile();
        cacheFile.setLastModified(fiftyYearHence());
    }


}
