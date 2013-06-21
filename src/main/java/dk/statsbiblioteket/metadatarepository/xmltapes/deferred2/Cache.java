package dk.statsbiblioteket.metadatarepository.xmltapes.deferred2;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Cache is a fedora storage implementation that just holds files in a cache dir. Writes happen to the tempdir, and
 * when the stream is closed, the file is moved to the cache dir. Read operations are resolved against the cache dir,
 * and if not found delegated.
 */
public class Cache extends AbstractDeferringArchive{

    public static final String TEMP_PREFIX = "temp";
    private final File tempDir;


    public Cache( File cacheDir, File tempDir) {
        super();
        super.setDeferredDir(cacheDir);
        this.tempDir = tempDir;
    }

    private File getTempFile(URI id) throws IOException {
        File tempfile = File.createTempFile(id.toString(), TEMP_PREFIX, tempDir);
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
