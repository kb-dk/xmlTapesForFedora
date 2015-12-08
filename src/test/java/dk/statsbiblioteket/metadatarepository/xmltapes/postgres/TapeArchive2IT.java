package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import static org.junit.Assert.assertEquals;

import dk.statsbiblioteket.metadatarepository.xmltapes.TestNamesListener;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractTapeArchive2IT;

import dk.statsbiblioteket.metadatarepository.xmltapes.postgres.PostgresTestSettings;
import org.testng.annotations.Listeners;

public class TapeArchive2IT extends AbstractTapeArchive2IT {

    @Override
    protected Index getIndex() {
        return PostgresTestSettings.getIndex();
    }

}
