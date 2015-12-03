package dk.statsbiblioteket.metadatarepository.xmltapes.common.index;

public class IndexErrorException extends RuntimeException {

    public IndexErrorException(String message) {
        super(message);
    }
    
    public IndexErrorException(String message, Throwable t) {
        super(message, t);
    }
    
    
}
