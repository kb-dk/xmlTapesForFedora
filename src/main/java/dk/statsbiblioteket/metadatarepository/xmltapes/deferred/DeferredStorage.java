package dk.statsbiblioteket.metadatarepository.xmltapes.deferred;

import dk.statsbiblioteket.metadatarepository.xmltapes.TapeArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/18/13
 * Time: 2:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class DeferredStorage implements Archive {

    private Cache cache;


    private Archive archive;

    public DeferredStorage(Archive archive) {
        this.archive = archive;
        cache = new Cache(archive);
    }

    @Override
    public InputStream getInputStream(URI id) throws FileNotFoundException, IOException {
        DeferredOutputStream deferred = getCachedVersion(id);
        if (deferred != null){
            return deferred.getContents();
        }
        return archive.getInputStream(id);
    }

    @Override
    public boolean exist(URI id) {
        DeferredOutputStream deferred = getCachedVersion(id);
        if (deferred != null){
            return true;
        }
        return archive.exist(id);
    }

    @Override
    public long getSize(URI id) throws FileNotFoundException, IOException {
        DeferredOutputStream deferred = getCachedVersion(id);
        if (deferred != null){
            return deferred.size();
        }
        return archive.getSize(id);
    }

    @Override
    public OutputStream createNew(URI id, long estimatedSize) throws IOException {
        return cache.createNew(id, estimatedSize);
    }

    @Override
    public void remove(URI id) throws IOException {
        cache.remove(id);
        archive.remove(id);
    }

    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        return new CachedIterator(cache,archive,filterPrefix);
    }

    @Override
    public Index getIndex() {
        return archive.getIndex();
    }

    @Override
    public void setIndex(Index index) {
        archive.setIndex(index);
    }

    @Override
    public void init() throws IOException {
        archive.init();
    }

    @Override
    public void rebuild() throws IOException {
        archive.rebuild();
    }

    private DeferredOutputStream getCachedVersion(URI id){
        DeferredOutputStream deferred = cache.get(id);
        if (deferred != null && deferred.isClosed()){
            return deferred;
        }
        return null;

    }

}
