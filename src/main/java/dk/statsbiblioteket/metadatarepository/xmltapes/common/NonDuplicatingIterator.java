package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeUtils;

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

    private int currentCollection = -1;

    private Iterator<URI> currentIterator;

    private URI next;

    private final Set<URI> deletedIDs;

    public NonDuplicatingIterator(Iterator<URI> last, Collection<URI>... collections) {
        this.last = last;
        this.collections = collections;

        deletedIDs = new HashSet<URI>();
        nextIterator();
    }

    private void loopUntilNext(){
        while (next == null){
            if (!currentIterator.hasNext()){
                if (! nextIterator()) {
                      throw new NoSuchElementException();
                } else {
                    continue;
                }
            }
            next = currentIterator.next();
            if (next.toString().endsWith(TapeUtils.NAME_SEPARATOR+TapeUtils.DELETED)){

                next = URI.create(next.toString().replaceAll(Pattern.quote(
                        TapeUtils.NAME_SEPARATOR+TapeUtils.DELETED),
                        ""));
                deletedIDs.add(next);
            }
            if (deletedIDs.contains(next)){
                next = null;
                continue;
            }
            for (int i = 0; i < currentCollection; i++){
                if (collections[i].contains(next)){
                    next = null;
                    continue;
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
            URI temp = next;
            next = null;
            return temp;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
