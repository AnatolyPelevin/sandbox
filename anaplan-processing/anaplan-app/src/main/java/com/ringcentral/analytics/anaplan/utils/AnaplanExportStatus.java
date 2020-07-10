package com.ringcentral.analytics.anaplan.utils;

public enum AnaplanExportStatus {
    NOT_STARTED, //The task is scheduled to run, but has not started yet.
    IN_PROGRESS, //The task is currently running.
    COMPLETE, //The task has finished running whether successful or not.
    CANCELING, //The task is currently being canceled. Cancellation is not yet complete.
    CANCELED; //The task has been canceled and the changes have been rolled back.

}
