package dk.statsbiblioteket.metadatarepository.xmltapes.deferred2;

import dk.statsbiblioteket.util.Files;
import org.apache.commons.io.FileUtils;

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


    private final FileOutputStream stream;
    private final File tempFile;
    private final File cacheFile;

    public CacheOutputStream(File tempFile, File cacheFile) throws FileNotFoundException {
        this.tempFile = tempFile;
        this.cacheFile = cacheFile;
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
        Files.move(tempFile,cacheFile,true);
    }
}
