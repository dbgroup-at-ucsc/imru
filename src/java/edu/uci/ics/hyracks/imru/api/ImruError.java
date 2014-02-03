package edu.uci.ics.hyracks.imru.api;

public class ImruError extends Error {
    public ImruError(Exception e) {
        super(e);
    }
}
