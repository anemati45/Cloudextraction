/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import faasinspector.fiResponse;
import java.util.List;

/**
 *
 * @author wlloyd
 */
public class Response extends fiResponse {

    //
    // User Defined Attributes
    //
    //
    // ADD getters and setters for custom attributes here.
    //
    private boolean success;
    private String bucketname;
    private String tablename;
    public String dbname;

    private String error;

    public String getBucketname() {
        return bucketname;
    }

    public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public String getDbname() {
        return this.dbname;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public boolean getSuccess() {
        return success;
    }

    public void setSuccess(boolean message) {
        this.success = message;
    }

    @Override
    public String getError() {
        return error;
    }

    String filesave_filtering, filesave_aggregate;

    public String getFname_filtering() {
        return this.filesave_filtering;
    }

    public void setFname_filtering(String filesave_filtering) {
        this.filesave_filtering = filesave_filtering;
    }

    public String getFname_aggregate() {
        return this.filesave_aggregate;
    }

    public void setFname_aggregate(String filesave_aggregate) {
        this.filesave_aggregate = filesave_aggregate;
    }

    @Override
    public void setError(String error) {
        this.error = error;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Bucketname=");
        sb.append(this.getBucketname());
        sb.append(" | ");
//        sb.append("dbname=");
//        sb.append(this.getDbname());
//        sb.append(" | ");
//        sb.append("tablename=");
//        sb.append(this.getTablename());
//        sb.append(" | ");
//        sb.append("fileName1=");
        sb.append(this.getFname_filtering());
        sb.append(" | ");
        sb.append("fileName2=");
        sb.append(this.getFname_aggregate());

        return sb + super.toString();
    }

}
