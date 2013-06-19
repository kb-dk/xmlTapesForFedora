package dk.statsbiblioteket.metadatarepository.xmltapes.deferred;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/18/13
 * Time: 3:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class Cache {

    private static final Logger log = LoggerFactory.getLogger(Cache.class);

    Map<URI,DeferredOutputStream> cached;
    private Archive archive;

    public Cache(Archive archive){
        this(archive,5000);
    }

    public Cache(Archive archive, long delay) {
        this.archive = archive;
        cached = new HashMap<URI, DeferredOutputStream>();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    saveAll();
                } catch (IOException e) {
                    log.error("Failed to save object from cache",e);
                }
            }
        },delay,delay);
    }

    public synchronized DeferredOutputStream createNew(URI id, long estimatedSize) {
        DeferredOutputStream value = new DeferredOutputStream(id, estimatedSize);
        cached.put(id, value);
        return value;
    }


    public synchronized DeferredOutputStream get(URI id){
        return cached.get(id);
    }


    public synchronized void remove(URI id) {
        cached.remove(id);
    }

    private synchronized void saveAll() throws IOException {
        Collection<DeferredOutputStream> closed = getAllClosed();

        for (DeferredOutputStream deferredOutputStream : closed) {
            if (deferredOutputStream.isClosed()){
                OutputStream tapeOutput = archive.createNew(deferredOutputStream.getId(), deferredOutputStream.size());
                IOUtils.copyLarge(deferredOutputStream.getContents(),tapeOutput);
                tapeOutput.close();
            }
        }
        for (DeferredOutputStream deferredOutputStream : closed) {
            cached.remove(deferredOutputStream.getId());
        }
    }

    private Collection<DeferredOutputStream> getAllClosed() {
        List<DeferredOutputStream> result = new ArrayList<DeferredOutputStream>();
        for (DeferredOutputStream deferredOutputStream : cached.values()) {
            if (deferredOutputStream.isClosed()){
                result.add(deferredOutputStream);
            }
        }

        Collections.sort(result,new Comparator<DeferredOutputStream>() {
            @Override
            public int compare(DeferredOutputStream o1, DeferredOutputStream o2) {
                return Long.compare(o1.getLastModified(),o2.getEstimatedSize());
            }
        });
        return result;
    }

    public Set<URI> getIDs(){
        return new HashSet<URI>(cached.keySet());

    }
}
