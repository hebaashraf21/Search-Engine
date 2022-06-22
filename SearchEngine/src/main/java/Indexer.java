import java.io.*;
import java.util.*;
import org.jsoup.Jsoup;
import org.tartarus.snowball.ext.PorterStemmer;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.jsoup.Connection;
//import org.apache.lucene.analysis.PorterStemmer;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
//import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.bson.types.ObjectId;
import static com.mongodb.client.model.Filters.eq;
public class Indexer {
    public static void main( String[] args ) throws IOException{
        //connect MongoDB
        String uri = "mongodb://localhost";
        MongoClient mongoclient = MongoClients.create(uri);
        //connect TestSearchIndex database
        MongoDatabase db = mongoclient.getDatabase("TestSearchIndex");
        //int TotalNumberOfPages=5032;
        //connect page
        //String ThisURL="https://en.wikipedia.org/wiki/History_of_slavery_in_Missouri";

        //Get The page conent
        String PageText="asd fg";
        String pageTextWords[]=PageText.split(" ");
        /////////////////////////////removing stop words////////////////////////
        ArrayList<String> StopWords = new ArrayList<String>();
        FileInputStream StopFile=new FileInputStream("C:\\Users\\user\\eclipse-workspace\\SearchEngine\\stopwords.txt");
        byte[] b = new byte[ StopFile.available()];

        StopFile.read(b);
        StopFile.close();
        String StopData[]=new String(b).trim().split("\n");

        for(int i=0;i<StopData.length;i++)
        {
            StopWords.add(StopData[i].trim());

        }
        MongoDatabase db2 = mongoclient.getDatabase("CrawlerResultDB");
        MongoCollection<org.bson.Document> CrawlingCollection = db2.getCollection("CrawlerResult");
        FindIterable<org.bson.Document> URLStoIndexiterable = CrawlingCollection.find();

        for(org.bson.Document URLstobeIndexed : URLStoIndexiterable)
        {

            String url=(String) URLstobeIndexed.get("url");

            //List<String> URLs= (List<String>) URLstobeIndexed.get("Array");


  //          for(int j=50;j<URLs.size();j++)
            if(true)
            {


                System.out.println("////////////////////"+url+"///////////////////////////////////");
                try {
                    Document doc =  Jsoup.connect(url).get();

                    //Get The page conent
                    PageText=doc.text();
                    pageTextWords=PageText.split(" ");
                    for (int i=0;i<pageTextWords.length;i++)
                    {
                        //Check if the word is not a stop word
                        if(!(StopWords.contains(pageTextWords[i].trim().toLowerCase())))
                        {
                            //Remove punctuation and numbers
                            pageTextWords[i]=pageTextWords[i].replaceAll("[^a-zA-Z ]", "");

                            //get the tags that contain that word in that document to use them in ranking
                            ArrayList<String> Tags = new ArrayList<String>();
                            if(!pageTextWords[i].equals("")) //because the replaceAll function may makes it empty string
                            {

                                Elements elts= doc.getElementsContainingOwnText(pageTextWords[i]);
                                for(Element e:elts)
                                {
                                    //check that the tag is not inserted before in that list
                                    if(!(Tags.contains(e.tagName())))
                                        Tags.add(e.tagName());
                                }


                            }

                            //Stemming
                            PorterStemmer stemmer = new PorterStemmer();
                            stemmer.setCurrent(pageTextWords[i].toLowerCase());
                            stemmer.stem();

                            pageTextWords[i]=stemmer.getCurrent();

                            if(!pageTextWords[i].equals(""))
                            {
                                //index here to Know the position

                                MongoIterable<String> table= db.listCollectionNames(); // Get all collections(words stored in the database)
                                boolean CollectionExist = false;
                                for(String s : table )
                                {
                                    if(s.equals(pageTextWords[i]))
                                    {
                                        //collection exists
                                        CollectionExist=true;
                                        break; //because there is only one collection for that word
                                    }
                                }
                                boolean URLExist = false;
                                if(CollectionExist)
                                {

                                    //collection exists
                                    MongoCollection<org.bson.Document> word = db.getCollection(pageTextWords[i]);
                                    //Get all URLs of this Word
                                    FindIterable<org.bson.Document> ThisWordDocuments = word.find();

                                    for(org.bson.Document URLDocument : ThisWordDocuments)
                                    {
                                        //Inside one document
                                        //Loop on each key-value pair
                                        //Break if URL Exists
                                        if(URLExist)
                                            break;//to break the outer loop if the inner loop breaks because the document of this URL exists only once
                                        for(Map.Entry<String,Object>e:URLDocument.entrySet())
                                        {
                                            if(e.getKey().equals("URL")&&e.getValue().toString().equals(url))
                                            {


                                                //If URL exists --> increase its then TF
                                                URLExist=true;
                                                //get The value of TF in that Document and increase it by one
                                                int newTF =(int) URLDocument.get("TF")+1;
                                                List<Integer> positions= (List<Integer>) URLDocument.get("positions");
                                                positions.add(i);
                                                List<String> OldTags=  (List<String>) URLDocument.get("Tags");
                                                List<String> NewTags= new ArrayList<>();
                                                NewTags.addAll(OldTags);
                                                NewTags.addAll(Tags);
                                                HashSet<String>UniqueTags = new HashSet<String>(NewTags);


                                                //Set the value of TF of the URL to old TF+1
                                                word.updateOne(Filters.eq("URL",url),Updates.set("TF",newTF));
                                                word.updateOne(Filters.eq("URL",url),Updates.set("positions",positions));
                                                word.updateOne(Filters.eq("URL",url),Updates.set("Tags",UniqueTags));
                                                break;

                                            }

                                        }
                                    }
                                    if(!URLExist)
                                    {
                                        org.bson.Document newdoc= new org.bson.Document ();
                                        newdoc.append("URL",url);
                                        newdoc.append("TF",1);
                                        List<Integer> positions = new ArrayList<>();
                                        positions.add(i);
                                        //positions to be used in phrase searching
                                        newdoc.append("positions", positions);
                                        newdoc.append("Tags", Tags);
                                        newdoc.append("weights", 0);
                                        word.insertOne(newdoc);
                                    }
                                }
                                else
                                {
                                    //New word

                                    MongoCollection<org.bson.Document> word = db.getCollection(pageTextWords[i]);
                                    org.bson.Document newdoc= new org.bson.Document ();
                                    newdoc.append("URL",url);
                                    newdoc.append("TF",1);
                                    List<Integer> positions = new ArrayList<>();
                                    positions.add(i);
                                    newdoc.append("positions", positions);
                                    newdoc.append("Tags", Tags);
                                    newdoc.append("weights", 0);
                                    word.insertOne(newdoc);

                                }
                            }

                        }
                    }


                    System.out.println("////////////////////"+url+"///////////////////////////////////");
                }
                catch(Exception e)
                {

                }
            }
        }
        MongoIterable<String> table= db.listCollectionNames();
        for(String s : table )
        {
            MongoCollection<org.bson.Document> wordindexed = db.getCollection(s);
            System.out.println(s);
            FindIterable<org.bson.Document> ThisIndexedWordDocuments = wordindexed.find();
            for(org.bson.Document IndexedURLDocument : ThisIndexedWordDocuments)
            {
                String ThisDocumentURL=(String) IndexedURLDocument.get("URL");
                List<String> IndexedTags=  (List<String>) IndexedURLDocument.get("Tags");
                int importance=0;

                if(IndexedTags.contains("title"))
                    importance=20;
                else if(IndexedTags.contains("head"))
                    importance=18;
                else if(IndexedTags.contains("strong"))
                    importance=16;
                else if(IndexedTags.contains("main"))
                    importance=14;
                else if(IndexedTags.contains("h1"))
                    importance=12;
                else if(IndexedTags.contains("h2"))
                    importance=10;
                else if(IndexedTags.contains("h3"))
                    importance=8;
                else if(IndexedTags.contains("h4"))
                    importance=6;
                else if(IndexedTags.contains("h5"))
                    importance=4;
                else if(IndexedTags.contains("h6"))
                    importance=2;
                else
                    importance=2;

                wordindexed.updateOne(Filters.eq("URL",ThisDocumentURL),Updates.set("weights",importance));

            }

        }
        // String PageTextWitoutStopWords="";


///////////////////////////////////////////////////////////////////////////////RANKER//////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
     /* String Query="\"the United States\"";

      //Check if it is between ""
      if(Query.charAt(0)=='"'&&Query.charAt(Query.length()-1)=='"')
      {
    	 // Phrase searching
    	  Query=Query.substring(1,Query.length()-1);
    	  String Original=Query;
    	  String QueryWords[]=Query.split(" ");// To be processed to search by it in the data base
    	  ArrayList<String>URLResultsLists= new ArrayList<>(); //To be filled with URLs containing this query(if any)
    	  ArrayList<Integer>WeightList= new ArrayList<>();//To be filled with corresponding TF-IDF of URLs containing this query(if any)
    	  ArrayList<Integer>PhraseFrequency= new ArrayList<>();
    	  ArrayList<Integer>PageRank= new ArrayList<>();
    	  ArrayList<ArrayList<String>> ListOfURLLists=new ArrayList<ArrayList<String>>();
    	  boolean AllWordsExist = true;

    	  int FirstNonStopWord=-1;
    	  boolean ThereIsANonStopWord=false;
    	  for (int i=0;i<QueryWords.length;i++)//Check that each word stem exists in the data base
    	  {
    		  if(!(StopWords.contains(QueryWords[i].trim().toLowerCase())))
    		  {

	    		  if(QueryWords[i]!="")
	    			  QueryWords[i]=QueryWords[i].replaceAll("[^a-zA-Z ]", "");
	    		  if(QueryWords[i]!="")
	    		  {
	    			  ThereIsANonStopWord=true;
	    			  if(FirstNonStopWord==-1)
	    			  {
	    				  FirstNonStopWord=i;
	    			  }
		    		  PorterStemmer stemmer = new PorterStemmer();
		              stemmer.setCurrent(QueryWords[i].toLowerCase());
		              stemmer.stem();
		              QueryWords[i]=stemmer.getCurrent();

		              ArrayList<String> ThisWordURLs=new ArrayList<String>();
		              boolean CollectionExist = false;
		              List<String> PhraseCollectionstable = db.listCollectionNames().into(new ArrayList<>());
		              if (PhraseCollectionstable.contains(QueryWords[i])) {
		            	//collection exists
		         			 CollectionExist=true;
		         			//All words stem are in the database
		           		  //Get the URLs of each word stem
		         			  MongoCollection<org.bson.Document> word = db.getCollection(QueryWords[i]);
		 		      		 //Get all URLs of this Word
		 		      		  FindIterable<org.bson.Document> ThisWordDocuments = word.find();

		 		      		  for(org.bson.Document URLDocument : ThisWordDocuments)
		 		      		  {
		 		      			ThisWordURLs.add((String) URLDocument.get("URL")) ;
		 		      		  }
		 		      		ListOfURLLists.add(ThisWordURLs);
		              }
		              if(!CollectionExist) //This word is not in the database
		         	  {
		         		  AllWordsExist=false;
		         		  break;
		            	}


	    	      }
    		  }
    	  }
    	  if(!AllWordsExist|| !ThereIsANonStopWord)
    	  {
    		  System.out.println("1 ");
    	  }
    	  else
    	  {

    		  //Get the intersection of the lists of URLs
    		  boolean UrlIntersection=true;
    		  for (int i=1;i<ListOfURLLists.size();i++)
    		  {
    			  ListOfURLLists.get(i).retainAll(ListOfURLLists.get(i-1));//Get the intersection of each list with the previous one
    			  if(ListOfURLLists.get(i).size()==0)//No intersection
    			  {
    				  UrlIntersection=false;
    				  break;
    			  }
    		  }
    		  if(!UrlIntersection)
    		  {
    			  System.out.println("2");
    		  }
    		  else
    		  {
    			  //There is intersection between URLs
    			  //The intersection is in the last list
    			  //ListOfLists.get(ListOfLists.size()-1);
    			  //For each URL --->parse it and check it contains the phrase
    			  for(int i=0;i<ListOfURLLists.get(ListOfURLLists.size()-1).size();i++)
    			  {
    				  //For each URL in the intersection
    				  //Get the content
    				  String URL=ListOfURLLists.get(ListOfURLLists.size()-1).get(i);
    				  Document Parsed = Jsoup.connect(URL).get();
    			      String PageContent=Parsed.text();
    			      if(PageContent.contains(Original))
    			      {
    			    	  URLResultsLists.add(URL);
    			    	  PhraseFrequency.add(PageText.split(Query, -1).length-1);
    			    	  MongoCollection<org.bson.Document> word = db.getCollection(QueryWords[FirstNonStopWord]);
    			    	  org.bson.Document URLDocument = word.find(eq("URL", new ObjectId(URL))).first();
    			    	  if (URLDocument != null) {
    			    	      int FirstWordWeight=(int) URLDocument.get("weights");
    			    	  			      				WeightList.add(FirstWordWeight);
    			    	  }


    			      }



    			  }
    		  }
  		   }


      }
      else
      {
    	  //Not phrase
    	  //get query words
    	  String QueryWords[]=Query.split(" ");// To be processed to search by it in the data base
    	  String OriginalQueryWords[]=Query.split(" "); //To store the original words without processing
    	  //To store URLs containing the word without stemming and store their info needed to rank
    	  ArrayList<String>URLLists= new ArrayList<>(); //To be filled with URLs containing this query(if any)
    	  ArrayList<Integer>TF_IDFLists= new ArrayList<>();//To be filled with corresponding TF-IDF of URLs containing this query(if any)
    	  ArrayList<Integer>No_Of_QueryWords_in_the_document= new ArrayList<>();
    	  ArrayList<Integer>PageRank= new ArrayList<>();
    	  ArrayList<Integer>WeightsList= new ArrayList<>();
    	//To store URLs containing the word with stemming and store their info needed to rank
    	  ArrayList<String>URLListsStem= new ArrayList<>(); //To be filled with URLs containing this query(if any)
    	  ArrayList<Integer>TF_IDFListsStem= new ArrayList<>();//To be filled with corresponding TF-IDF of URLs containing this query(if any)
    	  ArrayList<Integer>No_Of_QueryWords_in_the_documentStem= new ArrayList<>();
    	  ArrayList<Integer>PageRankStem= new ArrayList<>();
    	  ArrayList<Integer>WeightsListStem= new ArrayList<>();
    	  for (int i=0;i<QueryWords.length;i++)//Check that each word stem exists in the data base
    	  {
    		  //remove stop words from the query ,stem the rest of words and convert them to lower case letters
    		  if(!(StopWords.contains(QueryWords[i].trim().toLowerCase())))
    		  {
	    		  if(QueryWords[i]!="")
	    			  QueryWords[i]=QueryWords[i].replaceAll("[^a-zA-Z ]", "");
	    		  if(QueryWords[i]!="")
	    		  {
		    		  PorterStemmer stemmer = new PorterStemmer();
		              stemmer.setCurrent(QueryWords[i].toLowerCase());
		              stemmer.stem();
		              QueryWords[i]=stemmer.getCurrent();
		              //////////////////////////////////////////
		              List<String> Collectionstable = db.listCollectionNames().into(new ArrayList<>()); // Get all collections(words stored in the database)
		        	  boolean CollectionExist = false;
		        	  if (Collectionstable.contains(QueryWords[i]))
		         		  {
		         			//collection exists
		         			 CollectionExist=true;

		         		  }


		            	//collection exists

		         	  if(CollectionExist)
		         	  {
		         		  //get all documents in the collection of this word
		         		 MongoCollection<org.bson.Document> word = db.getCollection(QueryWords[i]);
		         		 FindIterable<org.bson.Document> ThisWordDocuments = word.find();
		         		 //DF = number of documents containing this word
		         		 long DF=word.countDocuments();
		         		 int IDF = TotalNumberOfPages/ (int)DF;
		         		 for(org.bson.Document URLDocument : ThisWordDocuments)
		         		 {
		         			int TF =(int) URLDocument.get("TF");
		         			int TF_IDF = TF*IDF;
		         			//Get Url stored in that document and retrieve the array of positions and array of tags in this document
		         			String URL= (String) URLDocument.get("URL");
		         			int wordWeigt=(int) URLDocument.get("weights");

		         			//check if a word from the query exists in the non stemming form in this URL(the URL exist in the list of the URLs containing the non stemming word)

		         			boolean URLExist=false;
		         			int index=0;
		         			//check if the URL contained before in the List of URLs containing a non stemmed word from the query
		         			for(int k=0;k<URLLists.size();k++)
		         			{
		         				if(URLLists.get(k)==URL)
		         				{//This URL exists in the result
		         					index=k;
		         					URLExist=true;
		         					break;
		         				}

		         			}

		         			if(URLExist)
		         			{//This URL exists in the result
		         				//increase the TF_IDF, the weight sum of words and the count of words in it
		         				//the page rank will not change
		         				int LastTF_IDF=TF_IDFLists.get(index);
		         				TF_IDF+=LastTF_IDF;
		         				TF_IDFLists.add(index,TF_IDF);

		         				int NumberOfwORDS=No_Of_QueryWords_in_the_document.get(index);
		         				NumberOfwORDS++;
		         				No_Of_QueryWords_in_the_document.add(index,NumberOfwORDS);

		         				int OldWeights=WeightsList.get(index);
		         				OldWeights+=wordWeigt;
		         				WeightsList.add(index,OldWeights);

		         			}
		         			/////////////////////////////////////////////
		         			else
		         			{
		         				//If the URL is not in the List of URLs containing a non stemming word --> the last words in the query is not in this URL in a non stemming form
		         				//Check if it contains this word(current word) without stemming

	         					boolean ContainsThisWordWithoutStemming=false;
	         					//parse URL to get its content to check if
	         					Document Parsed = Jsoup.connect(URL).get();
	          			        String PageContent[]=Parsed.text().split(" ");
	          			        //retrieve the list of positions of this stem in the URL
	          			        ArrayList<Integer> positions= (ArrayList<Integer>) URLDocument.get("positions");

	          			        for(int k=0;k<positions.size();k++)
	    		         	    {
	    		         		 //for each position of the stem check if the word in that position is the original word in the query(without processing)
	    		         		  if(PageContent[positions.get(k)].replaceAll( "[.,:\" ]","").trim().toLowerCase().equals(OriginalQueryWords[i].trim().toLowerCase()))
	    		         		  {
	    		         			 ContainsThisWordWithoutStemming=true;
	    		         			 break;
	    		         		  }
	    		         	    }
	          			        if(!ContainsThisWordWithoutStemming)
	          			        {
	          			        	//this URL doesn't contain this word without stemming
	          			        	//Search for it in the List of URL containing stemmed words
			         				index=0;
				         			for(int k=0;k<URLListsStem.size();k++)
				         			{
				         				if(URLListsStem.get(k)==URL)
				         				{//This URL exists in the result
				         					index=k;
				         					URLExist=true;
				         					break;
				         				}
				         			}
			         				if(URLExist)
				         			{
				         				//increase the TF_IDF and the count of words in it
				         				//the page rank will not change
				         				int LastTF_IDF=TF_IDFListsStem.get(index);
				         				TF_IDF+=LastTF_IDF;
				         				TF_IDFListsStem.add(index,TF_IDF);

				         				int NumberOfwORDS=No_Of_QueryWords_in_the_documentStem.get(index);
				         				NumberOfwORDS++;
				         				No_Of_QueryWords_in_the_documentStem.add(index,NumberOfwORDS);

				         				int OldWeights=WeightsListStem.get(index);
				         				OldWeights+=wordWeigt;
				         				WeightsListStem.add(index,OldWeights);

				         			}
			         				else
			         				{
			         					URLListsStem.add(URL);
				         				TF_IDFListsStem.add(TF_IDF);
				         				No_Of_QueryWords_in_the_documentStem.add(1);
				         				WeightsListStem.add(wordWeigt);
			         				}
	          			        }
	          			        else
	          			        {
	          			        	//The word contains that word without stemming
		          			        	//First check if it is in the list of URLListsStem
		          			        	//if so --> remove it from the URLListsStem and remove its data
	          			        	URLExist=false;
	          			        	index=0;
				         			for(int k=0;k<URLListsStem.size();k++)
				         			{
				         				if(URLListsStem.get(k)==URL)
				         				{//This URL exists in the result
				         					index=k;
				         					URLExist=true;
				         					break;
				         				}
				         			}
				         			if(URLExist)
				         			{
				         				//This URL exists in the URLListsStem
				         					//delete it
				         				URLListsStem.remove(index);
				         				TF_IDFListsStem.remove(index);
				         				No_Of_QueryWords_in_the_documentStem.remove(index);
				         				WeightsListStem.remove(index);
				         			}
	          			        	//insert it in URLLists and insert its data
	          			        	URLLists.add(URL);
			         				TF_IDFLists.add(TF_IDF);
			         				No_Of_QueryWords_in_the_document.add(1);
			         				WeightsList.add(wordWeigt);
	          			        }
		         			}
		         		 }
		         	  }
	    		  }
    		  }
    	  }
      }*/


    }
}
