package edu.uci.ics.hyracks.imru.elastic.wrapper;

import java.util.Random;
import java.util.Vector;


public class ImruState {
    public ImruWriter diskCache;
    public Vector memCache;
    public long parsedDataSize;
    public long uuid = new Random().nextLong();
}
