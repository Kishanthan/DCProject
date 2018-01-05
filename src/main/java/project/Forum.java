package project;

import java.util.ArrayList;
import java.util.List;

public class Forum {
    private String comment;
    private List<ForumReply> forumReply = new ArrayList<>();
    private String commentTime;
    private String ownerIp;

    public Forum(String comment, String timeStamp, String ownerIp) {
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

    public List<ForumReply> getForumReply(){
        return this.forumReply;
    }

    public List addForumReply(ForumReply forumReply){
        this.forumReply.add(forumReply);
        return this.forumReply;
    }

}
