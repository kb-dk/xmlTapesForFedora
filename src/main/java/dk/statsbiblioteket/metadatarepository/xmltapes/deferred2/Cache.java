package dk.statsbiblioteket.metadatarepository.xmltapes.deferred2;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;

/**
 * Cache is a fedora storage implementation that just holds files in a cache dir. Writes happen to the tempdir, and
 * when the stream is closed, the file is moved to the cache dir. Read operations are resolved against the cache dir,
 * and if not found delegated.
 */
public class Cache extends AbstractDeferringArchive{

    private static final Logger log = LoggerFactory.getLogger(Cache.class);


    public static final String TEMP_PREFIX = "temp";
    private final File tempDir;


    public Cache( File cacheDir, File tempDir) throws IOException {
        super();
        super.setDeferredDir(cacheDir);
        this.tempDir = tempDir.getCanonicalFile();
        this.tempDir.mkdirs();
    }

    private File getTempFile(URI id) throws IOException {
        File tempfile = File.createTempFile(URLEncoder.encode(id.toString(), UTF_8), TEMP_PREFIX, tempDir);
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
        setDeleted(cacheFile);

    }


}
