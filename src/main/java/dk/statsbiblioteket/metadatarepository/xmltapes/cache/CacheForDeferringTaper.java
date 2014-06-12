package dk.statsbiblioteket.metadatarepository.xmltapes.cache;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.AbstractDeferringArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.AkubraCompatibleArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Cache is a fedora storage implementation that just holds files in a cache dir. Writes happen to the tempdir, and
 * when the stream is closed, the file is moved to the cache dir. Read operations are resolved against the cache dir,
 * and if not found delegated.
 */
public class CacheForDeferringTaper extends AbstractDeferringArchive<AkubraCompatibleArchive> implements AkubraCompatibleArchive{

    private static final Logger log = LoggerFactory.getLogger(CacheForDeferringTaper.class);


    private final File tempDir;

    public CacheForDeferringTaper(File cacheDir, File tempDir) throws IOException {
        super();
        super.setDeferredDir(cacheDir);
        this.tempDir = tempDir.getCanonicalFile();
        this.tempDir.mkdirs();

    }

    @Override
    public String toString() {
        return "CacheForDeferringTaper{" +
               "tempDir=" + tempDir.getName() +
               '}';
    }

    @Override
    public OutputStream createNew(URI id, long estimatedSize) throws IOException {
        testClosed();
        log.debug("Calling createNew with arguments {}",id);
        File tempFile = getTempFile(id,tempDir);
        return new CacheOutputStream(tempFile, getDeferredFile(id), lockPool);
    }



    @Override
    public  void remove(URI id) throws IOException {
        testClosed();
        log.debug("Calling remove with arguments {}",id);
        getDelegate().remove(id);
        log.debug("End of remove with arguments {}",id);

    }
}
