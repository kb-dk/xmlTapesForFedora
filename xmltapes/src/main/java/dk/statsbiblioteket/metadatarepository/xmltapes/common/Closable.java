package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 8/21/13
 * Time: 11:01 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Closable {

    private boolean closed = false;


    public void testClosed() {
        if (isClosed()){
            throw new IllegalStateException("Archive is closed");
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() throws IOException {
        closed = true;
    }
}
