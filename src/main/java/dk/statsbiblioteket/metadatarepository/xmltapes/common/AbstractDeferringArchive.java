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
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
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

    public static final String TEMP_PREFIX = "temp";
    private static final Logger log = LoggerFactory.getLogger(AbstractDeferringArchive.class);

    public static final String UTF_8 = "UTF-8";
    private T delegate;
    private  File storeDir;

    public final LockPool lockPool;




    public AbstractDeferringArchive() {
        lockPool = new LockPoolImpl();
    }



    @Override
    public InputStream getInputStream(URI id) throws FileNotFoundException, IOException {
        testClosed();


        File cacheFile = getDeferredFile(id);
        try {
            return new GzipCompressorInputStream(new FileInputStream(cacheFile));
        } catch (FileNotFoundException e){
            return delegate.getInputStream(id);
        }
    }

    //TODO this method have grown in scope. Split, doc or do something
    public File getDeferredFile(URI id) throws IOException {
        final String filename = TapeUtils.toFilename(id);
        final File file = new File(storeDir, filename);
        final File fileNew = TapeUtils.toNewName(file);
        if (!file.exists() && fileNew.exists()) {
            return fileNew;
        }
        return file;
    }

    protected File getDeferredFileDeleted(URI id) {
        try {
            return new File(storeDir,
                    URLEncoder.encode(id.toString()+ "#"+TapeUtils.DELETED, UTF_8));
        } catch (UnsupportedEncodingException e) {
            throw new Error(e);
        }

    }


    @Override
    public boolean exist(URI id) throws IOException {
        testClosed();

        File cacheFile = getDeferredFile(id);
        if (cacheFile.exists()){
            return true;
        }

        File deleted = getDeferredFileDeleted(id);
        if (deleted.exists()){
            return false;
        }
        return  delegate.exist(id);
    }

    @Override
    public long getSize(URI id) throws FileNotFoundException, IOException {
        testClosed();
        lockPool.lockForWriting();
        try {
            File cacheFile = getDeferredFile(id);
            if (cacheFile.exists()) {
                return TapeUtils.getLengthUncompressed(cacheFile);
            }
        } finally {
            lockPool.unlockForWriting();
        }
        return delegate.getSize(id);


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
            List<File> newCacheFiles = FileFilterUtils.filterList((FileFilterUtils.prefixFileFilter("new_")),
                    FileUtils.listFiles(getStoreDir(), null, false));
            for (File newCacheFile : newCacheFiles) {
                final File fileOrig = new File(newCacheFile.getParent(),newCacheFile.getName().replaceFirst("^new_",""));
                if (fileOrig.exists()) {
                    FileUtils.deleteQuietly(newCacheFile);
                }  else {
                    FileUtils.moveFile(newCacheFile, fileOrig);
                }
            }


            cacheFiles = FileFilterUtils.filterList(FileFilterUtils.notFileFilter(FileFilterUtils.prefixFileFilter(
                    "new_")), FileUtils.listFiles(getStoreDir(), null, false));


            //TODO remove the _new files from this list
            // Sort them in last modified order
            Collections.sort(cacheFiles, new Comparator<File>() {
                @Override
                public int compare(File o1, File o2) {
                    long x = o1.lastModified();
                    long y = o2.lastModified();
                    return Long.valueOf(x).compareTo(y);
                }
            });
            return cacheFiles;
        } finally {
            lockPool.unlockForWriting();
        }
    }

    public Collection<URI> getCacheIDs(String filterPrefix) throws IOException {
        //Get the cached files
        List<File> cacheFiles = getStoreFiles();
        ArrayList<URI> result = new ArrayList<URI>();
        for (File cacheFile : cacheFiles) {
            URI id = TapeUtils.getIDfromFileWithDeleted(cacheFile);
            if (id == null){
                continue;
            }

            if (filterPrefix != null && id.toString().startsWith(filterPrefix)){
                result.add(id);
            }
        }
        return result;
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

    protected File getTempFile(URI id, File temp_dir) throws IOException {
        temp_dir.mkdirs();
        File tempfile = File.createTempFile(URLEncoder.encode(id.toString(), UTF_8), TapeUtils.GZ, temp_dir);
        tempfile.deleteOnExit();
        return tempfile;
    }



}


