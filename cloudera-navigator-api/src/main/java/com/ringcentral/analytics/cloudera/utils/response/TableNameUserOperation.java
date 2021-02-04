package com.ringcentral.analytics.cloudera.utils.response;

public class TableNameUserOperation {
    private String tableName;
    private String user;
    private String operation;


    public  TableNameUserOperation (TableResponse e){
        this.tableName = e.getResource().replaceAll(".+:", "").toUpperCase();
        this.user = e.getUsername();
        this.operation = e.getCommand();
    }

    @Override
    public String toString(){
        return tableName + "," + user + ", " + operation;
    }
}
