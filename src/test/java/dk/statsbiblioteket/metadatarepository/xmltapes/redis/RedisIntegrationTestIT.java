package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractIntegrationTestIT;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisTestSettings;

public class RedisIntegrationTestIT extends AbstractIntegrationTestIT {

    @Override
    protected Index getIndex() {
        return RedisTestSettings.getIndex();
    }
}
