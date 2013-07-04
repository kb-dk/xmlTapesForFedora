package dk.statsbiblioteket.metadatarepository.xmltapes.deferred2;

import dk.statsbiblioteket.util.Files;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/20/13
 * Time: 12:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class CacheOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(CacheOutputStream.class);

    private final FileOutputStream stream;
    private final File tempFile;
    private final File cacheFile;
    private LockPool lockPool;

    public CacheOutputStream(File tempFile, File cacheFile, LockPool lockPool) throws FileNotFoundException {
        this.tempFile = tempFile;
        this.cacheFile = cacheFile;
        this.lockPool = lockPool;
        stream = new FileOutputStream(tempFile);
    }

    @Override
    public void flush() throws IOException {
        stream.flush();
    }

    @Override
    public void write(int b) throws IOException {
        stream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        stream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        stream.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        stream.close();
        try {
            lockPool.acquireLock(cacheFile.getName());
            Files.move(tempFile,cacheFile,true);
            lockPool.releaseLock(cacheFile.getName());
        }
        catch (Exception e){
            log.warn("Tried to move temp to cache, caught Exception",e);
        }
    }
}
