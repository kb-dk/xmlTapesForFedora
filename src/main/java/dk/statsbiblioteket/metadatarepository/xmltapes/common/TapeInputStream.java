package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import java.io.IOException;
import java.io.InputStream;

/**
 * Exists to ensure that you run out of bytes when reading over the edge of the tar file entry. This is not checked
 * otherwise.
 */
public class TapeInputStream extends InputStream {
    private final InputStream tapeInputstream;
    private final long size;
    private long position = 0;


    public TapeInputStream(InputStream tapeInputstream, long size) {
        this.tapeInputstream = tapeInputstream;
        this.size = size;
    }

    @Override
    public int read() throws IOException {
        int data = tapeInputstream.read();
        position++;
        if (position >= size){
            return -1;
        }
        return data;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b,0,b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (size - position == 0){
            return -1;
        }
        if (len >= (size-position)){
            len = (int) (size-position);
        }
        int readBytes = tapeInputstream.read(b, off, len);
        position += readBytes;
        return readBytes;
    }

    @Override
    public long skip(long n) throws IOException {
        if (size - position == 0){
            return -1;
        }
        if (n >= (size-position)){
            n = (int) (size-position);
        }

        long bytes = tapeInputstream.skip(n);
        position += bytes;
        return bytes;
    }

    @Override
    public int available() throws IOException {
        return tapeInputstream.available();
    }

    @Override
    public void close() throws IOException {
        tapeInputstream.close();
    }

    @Override
    public void mark(int readlimit) {
        tapeInputstream.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
    }

    @Override
    public boolean markSupported() {
        return false;
    }
}
