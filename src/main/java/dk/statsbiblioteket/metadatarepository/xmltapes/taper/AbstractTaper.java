package dk.statsbiblioteket.metadatarepository.xmltapes.taper;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.AbstractDeferringArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.AkubraCompatibleArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.NonDuplicatingIterator;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeArchive;
import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 7/12/13
 * Time: 12:56 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractTaper extends AbstractDeferringArchive<TapeArchive> implements AkubraCompatibleArchive {



    protected AbstractDeferringArchive parent;
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(AbstractTaper.class);


    public AbstractTaper(File tapingDir) {
        super();
        super.setDeferredDir(tapingDir);

    }



    @Override
    public boolean exist(URI id) throws IOException {
        log.debug("Calling exist with id {}",id);
        return super.exist(id);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public long getSize(URI id) throws FileNotFoundException, IOException {
        log.debug("Calling getSize with id {}",id);
        return super.getSize(id);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public InputStream getInputStream(URI id) throws FileNotFoundException, IOException {
        log.debug("Calling getInputStream with arguments id {}",id);
        return super.getInputStream(id);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        log.debug("Calling listIDs with arguments {}",filterPrefix);
        return new NonDuplicatingIterator(getDelegate().listIds(filterPrefix),parent.getCacheIDs(filterPrefix),getCacheIDs(filterPrefix));
    }



    private void tapeTheTapingFileAddition(File fileToTape) throws IOException {
        log.debug("Taping addition of file {}",fileToTape.getName());
        lockPool.lockForWriting();
        try {
            URI id = getIDfromFile(fileToTape);
            getDelegate().tapeFile(id,fileToTape);
        } finally {
            FileUtils.deleteQuietly(fileToTape);
            lockPool.unlockForWriting();
        }


    }

    private synchronized void tapeTheTapingFileDeletion(File fileToTape) throws IOException {
        log.debug("Taping deletion of file {}",fileToTape);
        lockPool.lockForWriting();
        try {

            if (fileToTape.length() > 0){
                log.debug("File {} containted content, so add the content before deletion",fileToTape);
                tapeTheTapingFileAddition(fileToTape);
            }
            URI id = getIDfromFile(fileToTape);
            log.debug("Taping the file deletion {}",fileToTape);
            getDelegate().remove(id);

        } finally {
            FileUtils.deleteQuietly(fileToTape);
            lockPool.unlockForWriting();
        }
    }






    public void setParent(AbstractDeferringArchive parent) {
        this.parent = parent;
    }


}
