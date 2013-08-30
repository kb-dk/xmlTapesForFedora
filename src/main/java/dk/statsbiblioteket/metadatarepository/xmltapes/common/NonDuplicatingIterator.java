package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import org.slf4j.LoggerFactory;

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

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(NonDuplicatingIterator.class);


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
            //Check if the current iterator is empty
            if (!currentIterator.hasNext()){
                //Are there any more iterators to do?
                if (! nextIterator()) {
                      throw new NoSuchElementException();
                } else {
                    continue;
                }
            }
            //Get the next element
            next = currentIterator.next();
            log.debug("Getting element {} from collection {}",next,currentCollection);
            //If the next element is deleted
            if (next.toString().endsWith(TapeUtils.NAME_SEPARATOR+TapeUtils.DELETED)){
                //Translate to a normal (nondeleted) id and add id to deletedIDs
                next = URI.create(next.toString().replaceAll(Pattern.quote(
                        TapeUtils.NAME_SEPARATOR+TapeUtils.DELETED),
                        ""));
                deletedIDs.add(next);
            }
            //If the element is already marked as deleted
            if (deletedIDs.contains(next)){
                log.debug("Element {} was deleted, so skipping",next);
                //skip it
                next = null;
                continue;
            }
            //For all the collections we already have done
            for (int i = 0; i < currentCollection; i++){
                //If it contains the element
                if (collections[i].contains(next)){
                    //skip it
                    log.debug("We already have returned element {} from a previous collection, so skip it",next);
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
