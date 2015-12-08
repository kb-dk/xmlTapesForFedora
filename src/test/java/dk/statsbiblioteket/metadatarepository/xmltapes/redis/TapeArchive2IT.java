package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractTapeArchive2IT;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisTestSettings;


public class TapeArchive2IT extends AbstractTapeArchive2IT {

    @Override
    protected Index getIndex() {
        return RedisTestSettings.getIndex();
    }

}
