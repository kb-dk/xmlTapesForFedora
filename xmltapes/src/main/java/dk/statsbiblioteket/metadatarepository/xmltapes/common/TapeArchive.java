package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 8/20/13
 * Time: 12:22 PM
 * To change this template use File | Settings | File Templates.
 */
public interface TapeArchive extends Archive{


    void tapeFile(URI id, File fileToTape) throws IOException;

    void remove(URI id) throws IOException;


    /**
     * Get the index representation that this archive will use
     * @return
     */
    Index getIndex();

    void setIndex(Index index);

    boolean isFixErrors();

    void setFixErrors(boolean fixErrors);

    /**
     * Initialises and rebuild the index
     * @throws IOException
     */
    void rebuild() throws IOException;
}
