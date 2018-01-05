package project;

public class ForumReply {
    private String reply;
    private int order;
    private String comment;
    private String commentTime;
    private String ownerIp;

    public ForumReply(String comment, String timeStamp, String ownerIp) {
        this.comment = comment;
        this.commentTime = timeStamp;
        this.ownerIp = ownerIp;
    }

    public String getComment(){
        return this.comment;
    }
    public String getCommentTime(){
        return this.commentTime;
    }
    public String getOwnerIp(){
        return this.ownerIp;
    }

    public int getOrder(){
        return this.order;
    }

    public void setOrder(int order){
        this.order = order;
    }
}
