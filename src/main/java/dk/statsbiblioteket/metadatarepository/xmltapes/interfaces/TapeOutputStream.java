package dk.statsbiblioteket.metadatarepository.xmltapes.interfaces;

import de.schlichtherle.truezip.file.TVFS;
import dk.statsbiblioteket.metadatarepository.xmltapes.interfaces.StoreLock;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/22/13
 * Time: 12:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class TapeOutputStream extends OutputStream{

    private OutputStream delegate;
    private StoreLock writeLock;

    public TapeOutputStream(OutputStream delegate, StoreLock writeLock) {
        this.delegate = delegate;
        this.writeLock = writeLock;
        writeLock.lock(delegate);

    }

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
        TVFS.sync();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        writeLock.unlock(delegate);
    }
}
