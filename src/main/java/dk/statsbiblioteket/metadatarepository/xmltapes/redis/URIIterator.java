package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import java.net.URI;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/23/13
 * Time: 12:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class URIIterator implements Iterator<URI> {
    private Iterator<String> delegate;

    public URIIterator(Iterator<String> keys) {
        this.delegate = keys;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();

    }

    @Override
    public URI next() {
        return URI.create(delegate.next());

    }

    @Override
    public void remove() {
        delegate.remove();
    }
}
