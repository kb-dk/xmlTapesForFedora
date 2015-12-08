package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import dk.statsbiblioteket.metadatarepository.xmltapes.TestNamesListener;
import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractTapeArchiveIT;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.postgres.PostgresTestSettings;
import org.testng.annotations.Listeners;

public class TapeArchiveIT extends AbstractTapeArchiveIT {

    @Override
    protected Index getIndex() {
        return PostgresTestSettings.getIndex();
    }
}
