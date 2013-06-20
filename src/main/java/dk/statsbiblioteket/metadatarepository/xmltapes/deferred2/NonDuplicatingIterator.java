package dk.statsbiblioteket.metadatarepository.xmltapes.deferred2;

import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/20/13
 * Time: 12:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class NonDuplicatingIterator implements Iterator<URI> {

    private final Iterator<URI> last;
    private final Collection<URI>[] collections;

    private int currentCollection = 0;

    private Iterator<URI> currentIterator;

    private URI next;

    private final Set<URI> deletedIDs;

    public NonDuplicatingIterator(Iterator<URI> last, Collection<URI>... collections) {
        this.last = last;
        this.collections = collections;

        deletedIDs = new HashSet<URI>();
    }

    private void loopUntilNext(){
        next = null;
        while (next == null){
            if (!currentIterator.hasNext()){
                if (! nextIterator()) {
                      throw new NoSuchElementException();
                }
            }
            next = currentIterator.next();
            if (next.toString().endsWith("#DELETED")){

                next = URI.create(next.toString().replaceAll(Pattern.quote("#DELETED"), ""));
                deletedIDs.add(next);
            }
            if (deletedIDs.contains(next)){
                next = null;
                break;
            }
            for (int i = 0; i < currentCollection; i++){
                if (collections[i].contains(next)){
                    next = null;
                    break;
                }
            }
        }


    }

    @Override
    public boolean hasNext() {
        try {
            loopUntilNext();
            return true;
        } catch (NoSuchElementException e){
            return false;
        }
    }

    private boolean nextIterator() {
        currentCollection++;
        if (currentCollection > collections.length){
            return false;
        }
        if (currentCollection == collections.length){
            currentIterator = last;
            return true;
        } else {
            Collection<URI> coll = collections[currentCollection];
            currentIterator = coll.iterator();
            return true;
        }
    }

    @Override
    public URI next() {
        if (hasNext()){
            return next;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
