package ir;

import java.io.*;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.node.NodeBuilder.*;

 class DirectoryManager {
	
	private String dirPath;
	
	public DirectoryManager(String dirPath){
		this.dirPath = dirPath;
	}
	
	public File[] getDirFiles(){
		File file = new File(dirPath);
		File[] files = file.listFiles();
		System.out.println("read files --successful");
		return files;
	}
}

public class IndexManager{
	public static void main(String args[]) throws Exception{
		
		//constants
		String index = "team_s";
		String type = "document";
        String dirPath = "/home/sivaram/eclipse/files/AP_DATA/hw3/";
        
        //read LinkGraph from disk and deserialize
  		File toRead = new File(dirPath+"LinkGraph");
        FileInputStream fis = new FileInputStream(toRead);
        ObjectInputStream ois = new ObjectInputStream(fis);
        HashMap<String, Link> LG = (HashMap<String, Link>)ois.readObject();
  		
		DirectoryManager dm = new DirectoryManager(dirPath+"1/");
		File[] files = dm.getDirFiles();

		//creating index
		Node node = nodeBuilder().client(true).clusterName("macbook").node();
		Client client = node.client();
        
        // 3) Generate <Doc>s from files
        String[] docs;
        for(File file: files){
        	docs = getDocs(file);
	        
	        // 4) index documents
	        int i = 1;
	        String url, head, text, raw, inLinks = "", outLinks = "";
	        for(String doc : docs){
	        	url = StringUtils.substringBetween(doc, "<URL>", "</URL>");
	        	head = StringUtils.substringBetween(doc, "<HEAD>", "</HEAD>");
	        	text = StringUtils.substringBetween(doc, "<TEXT>", "</TEXT>");
	        	raw = StringUtils.substringBetween(doc, "<RAW>", "</RAW>");
	        	inLinks = parse(LG.get(url).getIn());
	        	outLinks = parse(LG.get(url).getOut());
	        	
	        	//if url exists, update links
	        	QueryBuilder qb = matchQuery("_id", url);
	        	HashMap<String, String> hm = search(client, qb, index, type); 
	        	if(!hm.isEmpty())
	        	{
	        		String ret_InLinks = hm.get("inLinks");
		        	String ret_OutLinks = hm.get("outLinks");
		        	//update links
		        	inLinks = Union(inLinks, ret_InLinks);
		        	outLinks = Union(outLinks, ret_OutLinks);
		        	System.out.println("updated links");
	        	}
	        	
	        	XContentBuilder doc_json = XContentFactory.jsonBuilder()
	        		    .startObject()
	        	        .field("URL", url)
	        	        .field("HEAD", head)
	        	        .field("TEXT", text)
	        	        .field("HTML", raw)
	        	        .field("INLINKS", inLinks)
	        	        .field("OUTLINKS", outLinks)
	        	        .endObject();
				client.prepareIndex(index, type, url)
					    .setSource(doc_json)
					    .execute()
					    .actionGet();
			
			System.out.println(i++ + "." + url);
			}
        }
	        System.out.println("index documents --successful");
	        
			//close client
			node.close();
	        
	}
	
	 public static String Union(String Links, String ret_Links)
	 {
		 String LINKS = "";
		 String[] link_arr = Links.split(" ");
		 String[] ret_link_arr = ret_Links.split(" ");
		 HashSet<String> hs = new HashSet<String>();
		 for(String str: link_arr)
			 hs.add(str);
		 for(String str: ret_link_arr)
			 hs.add(str);
		 //return final updated links
		 for(String str: hs){
			 LINKS += str + " ";
		 }
		 return LINKS.trim();
	 }
	 
	 public static String parse(String Link)
	 {
		 String res = "";
		 Link = StringUtils.substringBetween(Link, "[", "]");
		 String[] temp = Link.split(", ");
		 for(String str: temp)
			 res += str+ " ";		 
		 return res.trim();
	 }
	
		public static String[] getDocs(File file) throws IOException{
			 String strFromFile = FileUtils.readFileToString(file);//read file contents to string
			 return StringUtils.substringsBetween(strFromFile, "<DOC>", "</DOC>");
		 }
		
		 public static HashMap<String, String> search(Client client, QueryBuilder qb, String
         index, String type) {
			 HashMap<String, String> hm = new HashMap<String, String>();
			 SearchResponse scrollResp = client.prepareSearch(index).setTypes(type)
							         .setScroll(new TimeValue(6000))
							         .setQuery(qb)
							         .setExplain(true)
							         .setSize(1000).execute().actionGet();
         if(scrollResp.getHits().getTotalHits() == 0)
        	 return hm;
         else
         {
        	  for (SearchHit hit : scrollResp.getHits().getHits()) {
                  String inlinks =  (String) hit.getSource().get("INLINKS");
                  String outlinks =  (String) hit.getSource().get("OUTLINKS");
                  hm.put("inLinks", inlinks);
                  hm.put("outLinks", outlinks);
        	  }
        	  return hm;
         }
        	 
         }
	}
