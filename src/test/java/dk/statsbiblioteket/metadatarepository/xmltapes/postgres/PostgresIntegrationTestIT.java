package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractIntegrationTestIT;

public class PostgresIntegrationTestIT extends AbstractIntegrationTestIT {

    @Override
    protected Index getIndex() {
        return PostgresTestSettings.getIndex();
    }
}
