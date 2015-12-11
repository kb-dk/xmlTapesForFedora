package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 8/20/13
 * Time: 12:25 PM
 * To change this template use File | Settings | File Templates.
 */
public interface AkubraCompatibleArchive extends Archive{


    /**
     * Create a new blob and open an outputstream to populate it
     * @param id the id of the new blob
     * @param estimatedSize the estimated size of the content
     * @return an outputstream to the new blob
     * @throws java.io.IOException if creation of a new blob failed
     */
    OutputStream createNew(URI id, long estimatedSize) throws IOException;



    void remove(URI id) throws IOException;



}
