package dk.statsbiblioteket.metadatarepository.xmltapes.deferred2;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.Archive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
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
public abstract class AbstractDeferringArchive implements Archive{


    private Archive delegate;
    private  File deferredDir;



    public AbstractDeferringArchive() {
    }



    @Override
    public InputStream getInputStream(URI id) throws FileNotFoundException, IOException {

        File cacheFile = getDeferredFile(id);
        //HERE WE NEED TO RECOGNIZE THAT THE BLOB IS DEAD
        if (cacheFile.lastModified() > fortyYearHence()){
            throw new FileNotFoundException("Deleted file");
        }
        try {
            return new FileInputStream(cacheFile);
        } catch (FileNotFoundException e){
            return delegate.getInputStream(id);
        }
    }

    protected File getDeferredFile(URI id) {
        try {
            return new File(deferredDir,
                    URLEncoder.encode(id.toString(),"UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new Error(e);
        }

    }

    protected URI getIDfromFile(File cacheFile) {

        try {
            String name = cacheFile.getName();
            return new URI(URLDecoder.decode(name, "UTF-8"));
        } catch (URISyntaxException e) {
            return null;
        } catch (UnsupportedEncodingException e) {
            throw new Error(e);
        }
    }





    @Override
    public boolean exist(URI id) {
        File cacheFile = getDeferredFile(id);
        //HERE WE NEED TO RECOGNIZE THAT THE BLOB IS DEAD
        if (cacheFile.exists() && cacheFile.lastModified() > fortyYearHence()){
            return false;
        } else {
            return cacheFile.exists()  || delegate.exist(id);
        }
    }

    @Override
    public long getSize(URI id) throws FileNotFoundException, IOException {
        File cacheFile = getDeferredFile(id);
        long size = cacheFile.length();
        if (cacheFile.exists()){
            return size;
        }
        return delegate.getSize(id);
    }



    protected List<File> getCacheFiles() {
        //Get the cached files
        List<File> cacheFiles = new ArrayList<File>(FileUtils.listFiles(getDeferredDir(), null, false));
        // Sort them in last modified order
        Collections.sort(cacheFiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                long x = o1.lastModified();
                long y = o2.lastModified();
                return Long.compare(x, y);
            }
        });
        return cacheFiles;
    }

    protected Collection<URI> getCacheIDs(String filterPrefix) {
        //Get the cached files
        List<File> cacheFiles = getCacheFiles();
        ArrayList<URI> result = new ArrayList<URI>();
        for (File cacheFile : cacheFiles) {
            URI id = getIDfromFile(cacheFile);
            if (id == null){
                continue;
            }
            //HERE WE NEED TO RECOGNIZE THAT THE BLOB IS DEAD
            if (cacheFile.lastModified() > fortyYearHence()){
                id = URI.create(id.toString()+"#DELETED");
            }

            if (filterPrefix != null && id.toString().startsWith(filterPrefix)){
                result.add(id);
            }
        }
        return result;
    }



    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        return delegate.listIds(filterPrefix);
    }

    @Override
    public Index getIndex() {
        return delegate.getIndex();
    }

    @Override
    public void setIndex(Index index) {
        delegate.setIndex(index);
    }

    @Override
    public void init() throws IOException {
        delegate.init();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void rebuild() throws IOException {
        delegate.rebuild();
    }

    public File getDeferredDir() {
        return deferredDir;
    }


    public Archive getDelegate() {
        return delegate;
    }

    protected long fiftyYearHence(){
        return System.currentTimeMillis()+50*31558464000L;
    }

    protected long fortyYearHence(){
        return System.currentTimeMillis()+40*31558464000L;
    }

    public void setDelegate(Archive delegate) {
        this.delegate = delegate;
    }

    public void setDeferredDir(File deferredDir) {
        this.deferredDir = deferredDir;
    }
}
