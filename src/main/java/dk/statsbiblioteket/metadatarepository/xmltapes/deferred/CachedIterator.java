package dk.statsbiblioteket.metadatarepository.xmltapes.deferred;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;

import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/19/13
 * Time: 10:33 AM
 * To change this template use File | Settings | File Templates.
 */
public class CachedIterator implements Iterator<URI> {

    private final Set<URI> cachedIDs;
    private final Iterator<URI> it1;
    private final Iterator<URI> it2;
    private Cache cache;
    private final String filterPrefix;

    private URI next;


    public CachedIterator(Cache cache, Archive archive, String filterPrefix) {
        this.cache = cache;
        this.filterPrefix = filterPrefix;

        cachedIDs = cache.getIDs();
        if (filterPrefix != null){
            for (Iterator<URI> iterator = cachedIDs.iterator(); iterator.hasNext(); ) {
                URI next = iterator.next();
                if (!next.toString().startsWith(filterPrefix)){
                    iterator.remove();
                }
            }
        }
        it1 = cachedIDs.iterator();
        it2 = archive.listIds(filterPrefix);
    }

    @Override
    public boolean hasNext() {
        if (it1.hasNext()){
            return true;
        }
        while (true){
            if (it2.hasNext()){
                next = it2.next();
                if (!cachedIDs.contains(next)){
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    @Override
    public URI next() {
        if (it1.hasNext()){
            return it1.next();
        }
        if (next == null){
            if (hasNext()){
                URI tmp = next;
                next = null;
                return tmp;
            }
        }
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
