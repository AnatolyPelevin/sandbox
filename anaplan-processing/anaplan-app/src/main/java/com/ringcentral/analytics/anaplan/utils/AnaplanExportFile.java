package com.ringcentral.analytics.anaplan.utils;

import com.ringcentral.analytics.anaplan.AnaplanException;

import java.util.Optional;

public class AnaplanExportFile {
    private final String workspaceID;
    private final String modelID;
    private final String exportID;
    private final String fileID;

    public AnaplanExportFile(String workspaceID, String modelID, String exportID, String fileID) {
        this.workspaceID = workspaceID;
        this.modelID = modelID;
        this.exportID = exportID;
        this.fileID = fileID;
    }


    public void anaplanExportFileCheck(AnaplanOperation anaplanOperation) {
        if (!Optional.ofNullable(this.workspaceID).isPresent()){
            throw new AnaplanException("Can perform " + anaplanOperation.name() + ". Do not have workspaceID!");
        }
        if (!Optional.ofNullable(this.modelID).isPresent()){
            throw new AnaplanException("Can perform " + anaplanOperation.name() + ".  Do not have modelID!");
        }
        if (!Optional.ofNullable(this.exportID).isPresent()){
            throw new AnaplanException("Can perform " + anaplanOperation.name() + ".  Do not have exportID!");
        }
        if (!Optional.ofNullable(this.fileID).isPresent()){
            throw new AnaplanException("Can perform " + anaplanOperation.name() + ".  Do not have token!");
        }
    }

    public void setTaskID(String taskID) {
        this.taskID = taskID;
    }

    private String taskID;

    public String getWorkspaceID() {
        return workspaceID;
    }

    public String getModelID() {
        return modelID;
    }

    public String getExportID() {
        return exportID;
    }

    public String getFileID() {
        return fileID;
    }

    public String getTaskID() {
        return taskID;
    }
}
