package dk.statsbiblioteket.metadatarepository.xmltapes.taper;

import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 8/20/13
 * Time: 2:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class DirectTaper extends AbstractTaper {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(DirectTaper.class);

    public DirectTaper(File tapingDir) {
        super(tapingDir);
    }

    @Override
    public OutputStream createNew(URI id, long estimatedSize) throws IOException {
        log.debug("Calling createNew with arguments {}",id);
        File tempFile = getTempFile(id,getDeferredDir());
        return new DirectTaperOutputStream(tempFile, id,getDelegate());

    }

    @Override
    public void remove(URI id) throws IOException {
        getDelegate().remove(id);
    }
}
