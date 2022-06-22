import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import javax.print.Doc;
import java.util.*;
import java.util.logging.Filter;

public class DB {
    MongoClient client;
    String uri="mongodb://localhost:27017";
    MongoDatabase db;

    public DB(){
        client = MongoClients.create(uri);

    }

    public boolean updateCrawledPages(int cp){
        MongoDatabase db=client.getDatabase("CrawlerResultDB");
        MongoCollection state = db.getCollection("CrawlingCollection_Links_Queue");
        state.updateOne(Filters.eq("_id","pages"),new Document("$set",new Document("crawledPagesNum", cp)));
        return true;

    }
    public boolean storeResultFromCrawler(CrawlerResult cr){
        this.db=client.getDatabase("CrawlerResultDB");
        MongoCollection col=db.getCollection("CrawlerResult");
        BasicDBObject doc_3 = new BasicDBObject("_id", cr.cs).append("url",cr.url).append("title",cr.title);
        col.insertOne( new Document(doc_3.toMap()));
        return true;
    }
    public boolean CheckForCS(String cs){
        this.db=client.getDatabase("CrawlerResultDB");
        MongoCollection col=db.getCollection("CrawlerResult");
        FindIterable<Document> it=col.find(Filters.eq("_id",cs));
        for(Document doc:it){
            return false;
        }
        return true;

    }

    public boolean storeCrawlerResult(){
        //
        MongoDatabase db=client.getDatabase("demo");
        MongoCollection coll=db.getCollection("searches");
        System.out.println("Entered Func");
        FindIterable<Document> it= coll.find();
        for(Document doc:it){
            System.out.println(doc);
        }
        return true;
    }



    public ArrayList<String> getCrawlerResult(){
        this.db=client.getDatabase("CrawlerResultDB");
        MongoCollection<org.bson.Document> CrawlingCollection = db.getCollection("CrawlerResult");
        FindIterable<org.bson.Document> URLStoIndexiterable = CrawlingCollection.find();
        for(Document doc:URLStoIndexiterable){
            System.out.println(doc.get("url"));
        }
        return new ArrayList<>();
    }
    public boolean insertLinks(LinkedList<String> links){
        this.db=client.getDatabase("CrawlerResultDB");
        try{
        MongoCollection collec=db.getCollection("CrawlingCollection_Links_Queue");
        collec.deleteOne(Filters.eq("_id","Links_Queue"));
        collec.deleteOne(Filters.eq("_id","pages"));

        //org.bson.Document document = new org.bson.Document("_id", "Links_Queue");
        BasicDBObject doc_3 = new BasicDBObject("_id", "Links_Queue").append("Array", links);
        collec.insertOne( new Document(doc_3.toMap()));
            BasicDBObject doc_4 = new BasicDBObject("_id", "pages").append("crawledPagesNum", 0);
            collec.insertOne( new Document(doc_4.toMap()));

            System.out.println("See the dB");

        }
        catch (Exception e){
            System.out.println("ERror when inserting the seeds :"+e);
            return false;
        }
        return true;
    }
    public boolean updateCrawlerLinks(LinkedList<String> newLinks){
        //
        MongoDatabase db=client.getDatabase("CrawlerResultDB");
        MongoCollection collec=db.getCollection("CrawlingCollection_Links_Queue");

        collec.deleteOne(Filters.eq("_id","Links_Queue"));

        //org.bson.Document document = new org.bson.Document("_id", "Links_Queue");
        BasicDBObject doc_3 = new BasicDBObject("_id", "Links_Queue").append("Array", newLinks);
        collec.insertOne( new Document(doc_3.toMap()));


        System.out.println("Entered Func");

        return true;
    }
    public int getCrawledPages(){
        MongoDatabase db=client.getDatabase("CrawlerResultDB");
        MongoCollection state = db.getCollection("CrawlingCollection_Links_Queue");
        FindIterable<Document> it=state.find(Filters.eq("_id","pages"));
        int count=0;
        for(Document doc:it){
            count=(Integer) doc.get("crawledPagesNum");
        }
        return count;

    }
    public ArrayList<String> getLinks(){
        this.db=client.getDatabase("CrawlerResultDB");
        MongoCollection state = db.getCollection("CrawlingCollection_Links_Queue");
        FindIterable<Document> it=state.find(Filters.eq("_id","Links_Queue"));
        ArrayList<String> result=new ArrayList<>();
        for(Document doc:it){
            System.out.println(doc);
            result.addAll((ArrayList<String>)doc.get("Array"));

            //for(Map.Entry<String ,Object> e:doc.entrySet())
            // result=(LinkedList<String>) e.getValue();
               // if(e.getKey()=="Array")
              //      System.out.println(e);
        }

        return result;
    }




}
