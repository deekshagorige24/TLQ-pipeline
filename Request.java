package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    String bucketname;
    String filename;
    int row;
    int col;

    public String getBucketname() { return bucketname; }

    public String getFilename() { return filename; }

    public int getRow() { return row; }

    public int getCol() { return col; }

    public void setBucketname(String bucketname) { this.bucketname=bucketname; }
    public void setFilename(String filename) { this.filename=filename;}
    public void setRow(int row) { this.row=row; }
    public void setCol(int col) { this.col=col; }

    public Request() {

    }
}
