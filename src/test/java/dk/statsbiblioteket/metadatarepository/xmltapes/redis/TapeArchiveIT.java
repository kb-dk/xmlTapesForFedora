package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractTapeArchiveIT;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisTestSettings;


public class TapeArchiveIT extends AbstractTapeArchiveIT {

    @Override
    protected Index getIndex() {
        return RedisTestSettings.getIndex();
    }
}
