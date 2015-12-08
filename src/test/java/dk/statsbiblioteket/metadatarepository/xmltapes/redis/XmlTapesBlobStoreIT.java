package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractXmlTapesBlobStoreIT;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisTestSettings;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/21/13
 * Time: 2:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class XmlTapesBlobStoreIT extends AbstractXmlTapesBlobStoreIT {

    @Override
    protected Index getIndex() {
        return RedisTestSettings.getIndex();
    }

}
