package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.util.Bytes;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class Hasher {

    public static String getHash(String uri) {
        try {
            return Bytes.toHex(MessageDigest.getInstance("MD5").digest(uri.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

}
