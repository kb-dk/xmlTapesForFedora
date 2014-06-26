package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/20/13
 * Time: 12:17 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract  class AbstractDeferringArchive<T extends Archive> extends Closable implements Archive{

    private static final Logger log = LoggerFactory.getLogger(AbstractDeferringArchive.class);

    private T delegate;
    private  File storeDir;

    public final LockPool lockPool;




    public AbstractDeferringArchive() {
        lockPool = new LockPool();
    }



    @Override
    public InputStream getInputStream(URI id) throws FileNotFoundException, IOException {
        testClosed();

        File cacheFile = TapeUtils.getStoredFile(storeDir, id);
        try {
            return new GzipCompressorInputStream(new FileInputStream(cacheFile));
        } catch (FileNotFoundException e){
            return delegate.getInputStream(id);
        }
    }


    @Override
    public boolean exist(URI id) throws IOException {
        testClosed();

        File cacheFile = TapeUtils.getStoredFile(storeDir, id);
        if (cacheFile.exists()){
            return true;
        }

        File deleted = TapeUtils.getStoredFileDeleted(storeDir, id);
        if (deleted.exists()){
            return false;
        }
        return  delegate.exist(id);
    }

    @Override
    public long getSize(URI id) throws FileNotFoundException, IOException {
        testClosed();
        try {
            return StreamUtils.uncompressAndCountBytes(TapeUtils.getStoredFile(storeDir, id));
        } catch (FileNotFoundException e){
            return delegate.getSize(id);
        }
    }


    /**
     * Get the files in the cache.
     * Note, this method moves the "new_" files to the right names if no original is found and delete it if an original is found
     * @return
     */
    public List<File> getStoreFiles() throws IOException {
        //Ensure that noone is able to lock files until we have locked all.
        List<File> cacheFiles;

        lockPool.lockForWriting();
        try {
            handleNew_Files();

            cacheFiles = FileFilterUtils.filterList(FileFilterUtils.notFileFilter(FileFilterUtils.prefixFileFilter(TapeUtils.NEW_)), FileUtils.listFiles(getStoreDir(), null, false));

            // Sort them in last modified order
            Collections.sort(cacheFiles, new Comparator<File>() {
                @Override
                public int compare(File o1, File o2) {
                    return Long.valueOf(o1.lastModified()).compareTo(o2.lastModified());
                }
            });
            return cacheFiles;
        } finally {
            lockPool.unlockForWriting();
        }
    }

    private void handleNew_Files() throws IOException {
        List<File> newCacheFiles = FileFilterUtils.filterList((FileFilterUtils.prefixFileFilter(TapeUtils.NEW_)),
                FileUtils.listFiles(getStoreDir(), null, false));
        for (File newCacheFile : newCacheFiles) {
            final File fileOrig = new File(newCacheFile.getParent(),newCacheFile.getName().replaceFirst("^"+ TapeUtils.NEW_,""));
            if (fileOrig.exists()) {
                FileUtils.deleteQuietly(newCacheFile);
            }  else {
                FileUtils.moveFile(newCacheFile, fileOrig);
            }
        }
    }


    @Override
    public Iterator<URI> listIds(String filterPrefix) throws IOException {
        testClosed();
        log.debug("Calling listIDs with argument {}",filterPrefix);
        return delegate.listIds(filterPrefix);
    }

    @Override
    public void init() throws IOException {
        delegate.init();
    }

    public File getStoreDir() {
        return storeDir;
    }


    public T getDelegate() {
        return delegate;
    }

    public void setDelegate(T delegate) {
        this.delegate = delegate;
    }

    public void setStoreDir(File storeDir) {
        try {
            this.storeDir = storeDir.getCanonicalFile();
            this.storeDir.mkdirs();
        } catch (IOException e) {
            this.storeDir = storeDir;
        }
    }


    public void close() throws IOException{
        super.close();
        getDelegate().close();
    }

    public boolean isClosed(){
        return super.isClosed() && getDelegate().isClosed();
    }
}


