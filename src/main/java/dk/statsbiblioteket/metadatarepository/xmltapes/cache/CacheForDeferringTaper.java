package dk.statsbiblioteket.metadatarepository.xmltapes.cache;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.AbstractDeferringArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.AkubraCompatibleArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
    public OutputStream createNew(URI id, long estimatedSize) throws IOException {
        log.debug("Calling createNew with arguments {}",id);
        File tempFile = getTempFile(id,tempDir);
        return new CacheOutputStream(tempFile, getDeferredFile(id), lockPool);
    }



    @Override
    public  void remove(URI id) throws IOException {
        log.debug("Calling remove with arguments {}",id);
        getDelegate().remove(id);
        log.debug("End of remove with arguments {}",id);

    }


    @Override
    public boolean exist(URI id) throws IOException {
        log.debug("Calling exist with id {}",id);
        return super.exist(id);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public long getSize(URI id) throws FileNotFoundException, IOException {
        log.debug("Calling getSize with id {}",id);
        return super.getSize(id);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public InputStream getInputStream(URI id) throws FileNotFoundException, IOException {
        log.debug("Calling getInputStream with arguments id {}",id);
        return super.getInputStream(id);    //To change body of overridden methods use File | Settings | File Templates.
    }
}
