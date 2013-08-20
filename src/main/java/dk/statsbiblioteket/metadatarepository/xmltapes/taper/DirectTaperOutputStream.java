package dk.statsbiblioteket.metadatarepository.xmltapes.taper;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeArchive;
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
public class DirectTaperOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(DirectTaperOutputStream.class);

    private final FileOutputStream stream;
    private final File tempFile;
    private final URI id;
    private final TapeArchive storage;

    public DirectTaperOutputStream(File tempFile, URI id, TapeArchive storage) throws
            FileNotFoundException {
        this.tempFile = tempFile;
        this.id = id;
        this.storage = storage;
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
        try {
            stream.close();
            storage.tapeFile(id,tempFile);
            FileUtils.deleteQuietly(tempFile);
        } catch (Exception e) {
            log.warn("Tried to tape file directly, but failed", e);
        }
    }
}
