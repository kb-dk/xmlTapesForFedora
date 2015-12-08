package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import dk.statsbiblioteket.metadatarepository.xmltapes.TestNamesListener;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractIntegrationTestIT;
import dk.statsbiblioteket.metadatarepository.xmltapes.postgres.PostgresTestSettings;
import org.testng.annotations.Listeners;

public class IntegrationTestIT extends AbstractIntegrationTestIT {

    @Override
    protected Index getIndex() {
        return PostgresTestSettings.getIndex();
    }
}
