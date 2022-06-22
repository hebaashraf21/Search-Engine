import java.io.*;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.panforge.robotstxt.RobotsTxt;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.Connection;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public class Crawler implements Runnable {
    // first let's define our variables
    int crawledPages; //count of crawled pages
    static final int MX_link_count = 5000; // How many urls we want to crawl?
    // list of visited links
    LinkedList<String> QueueLinks = new LinkedList<>();
    //list of compact strings
    //MongoDatabase db;
    DB db;
    // constructor , initializing important data members
    public Crawler(int n, LinkedList<String> QL, DB db) {
        System.out.println("Crawler Initiated!");
        this.crawledPages = n;
        this.QueueLinks = QL;
        this.db = db;
    }

    //Threads running function
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + " Started Crawling!");
        while (crawledPages < MX_link_count) //keep crawling till we reach Max_Count
            crawl();
        System.out.println(Thread.currentThread().getName() + " Finished Crawling!");
    }

    //Core Function for crawling
    public void crawl() {
        String lnk = "";
        try {
            //get a new link to crawl
            lnk = getLink();
            //Check that it's not excluded from Robot.txt
            boolean not_excluded_From_Robot = Robot_Check_Access(lnk);
            if (not_excluded_From_Robot) {
                //computeCompactString
                String cs = getCS(lnk);


                boolean not_repeated_CS=db.CheckForCS(cs);
                if (not_repeated_CS || cs == "") {     //unique compact string ==> unique URL page
                    Document doc = request(lnk);       //get document of the link
                    if (doc != null) {
                        synchronized (this) {       //Synced for list of hyperlinks (not atomic operation)
                            crawledPages++;

                            CrawlerResult cr=new CrawlerResult(lnk,cs,doc.title());
                            //store crawled page
                            this.db.storeResultFromCrawler(cr);


                            //update crawledPages in db
                            this.db.updateCrawledPages(crawledPages);

                            Elements hyperlinks = doc.select("a[href]");
                            LinkedList<String> hyperlnks = new LinkedList<>();  //a list of hyperlinks contained in this link
                            for (Element e : hyperlinks)
                                hyperlnks.add(e.absUrl("href"));
                            QueueLinks.addAll(hyperlnks);   //add all hyperlinks in this page to queue
                            if (QueueLinks.size() <= MX_link_count) {
                                //update listoflinks in db by    this queue

                            } else {
                                QueueLinks.subList(MX_link_count + 1, QueueLinks.size()).clear();
                                //update listoflinks in db by this queue
                                //List<String> list_of_links = new ArrayList<String>(QueueLinks);
                                //org.bson.Document doc_lnks = new org.bson.Document("_id", "ArrayList").append("Links_Queue", list_of_links);
                            }
                            this.db.updateCrawlerLinks(QueueLinks);
                            notifyAll();
                        }
                    }
                } else {
                    System.out.println("Requested site is is already crawled! (it's a duplicate): " + lnk);
                }
            } else {
                System.out.println("Requested site is excluded from Robot.txt!");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //function to get next link to be crawled
    private String getLink() throws InterruptedException {
        synchronized (this) {
            String lnk = "";
            while (lnk.isEmpty()) {
                if (QueueLinks.size() == 0)
                    try {
                        wait();//wait if queue is empty till at least one link is added
                    } catch (InterruptedException e) {
                        System.out.println("\nInterrupted!");
                    }
                else {
                    lnk = QueueLinks.remove();//extract a url and
                    return lnk;              //return it
                }
            }
        }
        return null;
    }

    private Document request(String url) {
        try {
            Connection con = Jsoup.connect(url); // initiate connection
            Document doc = con.get();

            if (con.response().statusCode() == 200) { // status code 200 means OK (Successful requset)
                System.out.println("\n ||||Thread ID: " + Thread.currentThread().getId() + " Recieved Webpage at " + url);
                String title = doc.title();
                System.out.println("site#" + crawledPages + " " + title);

                return doc; // return document
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    public static String getCS(String url) {
        Document doc = null;
        try {

            Connection con = Jsoup.connect(url); // initiate connection
            doc = con.get();

            if (con.response().statusCode() == 200) { // status code 200 means OK (Successful requset)
                String h = "", cs = "";
                Elements elements = doc.select("p");
                Elements elements2 = doc.select("ul");
                Elements elements3 = doc.select("h1");
                Elements elements4 = doc.select("h2");
                Elements elements5 = doc.select("h3");
                Elements elements6 = doc.select("h4");
                Elements elements7 = doc.select("a");
                Elements elements8 = doc.select("li");
                Elements elements9 = doc.select("h6");

                h += (elements.size() > 0) ? elements.get(0).wholeText() : "";
                h += (elements2.size() > 0) ? elements2.get(0).wholeText() : "";
                h += (elements3.size() > 0) ? elements3.get(0).wholeText() : "";
                h += (elements4.size() > 0) ? elements4.get(0).wholeText() : "";
                h += (elements5.size() > 0) ? elements5.get(0).wholeText() : "";
                h += (elements6.size() > 0) ? elements6.get(0).wholeText() : "";
                h += (elements7.size() > 0) ? elements7.get(0).wholeText() : "";
                h += (elements8.size() > 0) ? elements8.get(0).wholeText() : "";
                h += (elements9.size() > 0) ? elements9.get(0).wholeText() : "";

                if (h.length() >= 0) {
                    cs += h;
                } else {
                    return "";
                }
                return cs;
            }
        } catch (HttpStatusException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }


    public boolean Robot_Check_Access(String url) {

        URL u = null;
        try {
            u = new URL(url);
            String Host = u.getHost();
            InputStream robotsTxtStream = new URL("http://" + Host + "/robots.txt").openStream();
            RobotsTxt robotsTxt = RobotsTxt.read(robotsTxtStream);
            boolean hasAccess = robotsTxt.query("*", u.getPath());
            return hasAccess;
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}