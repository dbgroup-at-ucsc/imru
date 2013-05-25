package edu.uci.ics.hyracks.ec2;

public interface NodeCallback {
    public void run(HyracksNode node) throws Exception;
}