/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

/**
 *
 * @author wlloyd
 */
public class Request {

    String bucketname, tablename, dbname;


    public String getbucketname() {
        return bucketname;
    }

    public void setbucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    public String getdbname() {
        return dbname;
    }

    public void setdbname(String dbname) {
        this.dbname = dbname;
    }

    public String gettablename() {
        return tablename;
    }

    public void settablename(String tablename) {
        this.tablename = tablename;
    }

    public Request() {

    }
}
