package dk.statsbiblioteket.metadatarepository.xmltapes;

import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/17/13
 * Time: 4:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class ReadLock {
    private URI[] ids;

    public ReadLock(URI[] ids) {
        //To change body of created methods use File | Settings | File Templates.
        this.ids = ids;
    }

    public URI[] getIds() {
        return ids;
    }
}
