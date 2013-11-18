package edu.uci.ics.hyracks.imru.api;

public enum RecoveryAction {
    /**
     * accept the partial model for the next iteration
     */
    Accept,
    /**
     * discard the partial model and rerun this iteration
     */
    Rerun,
    /**
     * rerun the failed data and integrate the resulting models.
     */
    PartiallyRerun,
}
