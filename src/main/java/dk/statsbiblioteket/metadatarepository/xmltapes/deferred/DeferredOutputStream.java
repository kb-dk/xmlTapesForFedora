package dk.statsbiblioteket.metadatarepository.xmltapes.deferred;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/18/13
 * Time: 3:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class DeferredOutputStream extends OutputStream {


    private URI id;
    private long estimatedSize;
    private long lastModified;

    private ByteArrayOutputStream contents;

    private boolean closed;

    public DeferredOutputStream(URI id, long estimatedSize) {
        this.id = id;
        this.estimatedSize = estimatedSize;
        contents = new ByteArrayOutputStream();
        closed = false;
    }

    public URI getId() {
        return id;
    }

    public long getEstimatedSize() {
        return estimatedSize;
    }


    @Override
    public void write(int b) throws IOException {
        contents.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        contents.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        contents.write(b);
    }

    @Override
    public void flush() throws IOException {
        contents.flush();
    }

    @Override
    public void close() throws IOException {
        contents.close();
        closed = true;
        lastModified = System.currentTimeMillis();
    }

    public boolean isClosed() {
        return closed;
    }

    public InputStream getContents(){
        return new ByteArrayInputStream(contents.toByteArray());
    }

    public long size(){
        return contents.size();
    }

    public long getLastModified() {
        return lastModified;
    }
}
