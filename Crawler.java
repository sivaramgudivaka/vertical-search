package ir;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


class UrlHandler
{
	private String protocol;
	private String host;
	private String port;
	private String path;
	private String query;
	
	public UrlHandler(String url) throws MalformedURLException, URISyntaxException
	{
		url = url.replaceAll("(^http)/{2,}", "/").replaceAll("http:/+", "http://").replaceAll("https:/+", "https://").replaceAll("#.*", "");
		URL urlObj = new URL(url);
		protocol = urlObj.getProtocol();
		host = urlObj.getHost().toLowerCase();
		if(urlObj.getProtocol().equalsIgnoreCase("http"))
			port = "80";
		else if(urlObj.getProtocol().equalsIgnoreCase("https"))
			port = "443";
		else
			port = ""+urlObj.getPort();
		path = urlObj.getPath();
		query = urlObj.getQuery() == null? "": "?"+ urlObj.getQuery() ;
	}
	
	public String canonicalize() throws UnsupportedEncodingException
	{		
		host = host.replaceFirst(":"+port, "").toLowerCase();//remove port
		return protocol+"://"+host+path+query;
	}
	
	public String toAbsolute(String stub) throws UnsupportedEncodingException, MalformedURLException, URISyntaxException
	{
		if(stub.matches("http.*|HTTP.*"))
			return new UrlHandler(stub).canonicalize();
		else if(stub.startsWith("javascript")||stub.startsWith("#")||stub.startsWith("mailto"))
			return "-1";
		else if(stub.startsWith(".."))   //go to parent dir
		{
			String temp = path;
			while(stub.startsWith("../"))
			{	
				temp = temp.substring(0, temp.lastIndexOf("/"));
				if(temp.equals("/")){
					stub = stub.replaceAll("\\.\\./", "");
					break;
				}
				temp = temp.substring(0, temp.lastIndexOf("/")+1);
				stub = StringUtils.removeStart(stub, "../");
			}
			return new UrlHandler(protocol+"://"+host+temp+query+stub).canonicalize();
		}
		else if(stub.startsWith("//"))   //replace from host
			return new UrlHandler(protocol+":"+stub).canonicalize();
		else if(stub.startsWith("./")){
			stub = stub.replaceFirst("./", "/");
			return new UrlHandler(protocol+"://"+host+path.substring(0, path.lastIndexOf("/"))+query+stub).canonicalize();
		}			
		else if(stub.startsWith("/"))    //replace path
			return new UrlHandler(protocol+"://"+host+stub).canonicalize();
		else 
			return new UrlHandler(protocol+"://"+host+path.substring(0, path.lastIndexOf("/")+1)+query+stub).canonicalize();
	}
}


class Link implements Serializable{
	private ArrayList<String> in = new ArrayList<String>();
	private ArrayList<String> out = new ArrayList<String>();
	
	public String getIn(){
		return in.toString();
	}
	
	public String getOut(){
		return out.toString();
	}
	
	public void addToIn(String url){
		in.add(url);
	}	
	
	public void addToOut(String url){
		out.add(url);
	}
}


public class Crawler {

	public static void main(String[] args) throws Exception{
		
		String s1 = "http://en.wikipedia.org/wiki/Data_structure";
		String s2 = "https://en.wikipedia.org/?title=Binary_tree";
		String s3 = "http://cslibrary.stanford.edu/110/BinaryTrees.html";
		String s4 = "https://www.cs.auckland.ac.nz/software/AlgAnim/trees.html";
		
		HashMap<String, Integer> master = new HashMap<String, Integer>();
		
		//comparator
		Comparator<String> myComp = new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				int diff = master.get(s2) - master.get(s1);
				if(diff == 0)
					return s1.equals(s2)?0:-1;
				return diff;
			}
		};
		
		TreeSet<String> frontier = new TreeSet<String>(myComp);
		master.put(new UrlHandler(s1).canonicalize(), 0);
		master.put(new UrlHandler(s2).canonicalize(), 0);
		master.put(new UrlHandler(s3).canonicalize(), 0);
		master.put(new UrlHandler(s4).canonicalize(), 0);
		
		//add initial seeds to frontier
		frontier.addAll(master.keySet());
		
		HashSet<String> visited = new HashSet<String>();
		HashMap<String, Link> LG = new HashMap<String, Link>();
		
		//read
		int count = 0, fno = 1, fcount = 0;		
		while(count<20000 && !frontier.isEmpty())
		{
			String charsetName;
			//program start time
			long startTime = System.currentTimeMillis();
			String url = frontier.pollFirst();
			UrlHandler uh;
			try{
				uh = new UrlHandler(url);
			}catch(Exception e){
				System.out.println(e.getClass());
				continue;
			}
			url = uh.canonicalize();
			visited.add(url);
			master.remove(url);
			System.out.println(url);
			
			//crawl url
			Document doc = null;
			try{
			/*** code for robot allowance ***/
			if(!isAllowed(url))
				continue;
			URL U = new URL(url);
			URLConnection con = U.openConnection();
			con.setRequestProperty("User-Agent","Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.29 Safari/537.36");
			String Content_Type = con.getContentType();

			if (Content_Type == null)
				continue;

			String[] type_encod = Content_Type.split(" ");
			if (type_encod[0].startsWith("text/html")){
				InputStream input = con.getInputStream();
				if (type_encod.length >= 2)
					charsetName = type_encod[1].split("=")[1];
				else
					charsetName = "ISO-8859-1";
			doc = Jsoup.parse(input, charsetName, url);
			}
			else continue;
			}catch(Exception e){
				System.out.println(e.getClass());
				continue;
			}			
			
			//if page not english, continue
			Elements lang = doc.select("html[lang]");
			String eng = "";
			if(!lang.isEmpty()){
				eng = lang.first().attr("lang");
			if(!eng.matches("en.*"))
				continue;
			}
			
			Elements out_links = doc.select("a[href]");  //out links
			if(out_links.isEmpty())
				continue;
			Elements titles = doc.select("title");
			String title = titles.isEmpty()?"":titles.first().text();
			Elements bodies = doc.select("body");
			String body = bodies.isEmpty()?"":bodies.first().toString();
			String text = bodies.isEmpty()?"":bodies.first().text();
			String child_url;
			
			for(Element e: out_links)
			{
				
				try{
					child_url = uh.toAbsolute(e.attr("href"));
				}catch(Exception e1){
					continue;
				}
				
				 //invalid url
				if(child_url.equals("-1"))
					continue;
				
				//already crawled
				if(visited.contains(child_url)){
					//update in count
					Link l = LG.get(child_url);
					l.addToIn(url);
					LG.put(child_url, l);
					continue;
				}					
				
				if(master.containsKey(child_url))
				{
					//update the in count. don't add to frontier
					int inc = master.get(child_url);
					master.put(child_url, inc+1);
					
					//update LG
					Link l;
					if(LG.containsKey(child_url))
						l = LG.get(child_url);
					else
						l = new Link();
						l.addToIn(url);
					LG.put(child_url, l);
				}
				else
				{
					//add to master
					master.put(child_url, 1);
					//add also to the frontier
					frontier.add(child_url);
					
					//update LG
					Link l = new Link();
					l.addToIn(url);
					LG.put(child_url, l);
				}
				
				//update LinkGraph with out-links
				Link l;
				if(LG.containsKey(url))
					l = LG.get(url);
				else
					l = new Link();
				
					l.addToOut(child_url);
					LG.put(url, l);
			}
			
			++fcount;
			//write to file
			if(fcount == 101){
				++fno;
				fcount = 1;
			}
			
			writeToFile(""+fno, title, text, body, url, charsetName);
			
			++count;
			
			//time taken to run
			long time = System.currentTimeMillis() - startTime;
			
			//sleep for at most 1 second
			if(time < 1000)
				Thread.sleep(1000-time);
			System.out.println(count);
		}
		
		//serialize LG
		File catalog = new File("/home/sivaram/eclipse/files/AP_DATA/hw3/LinkGraph");
	    FileOutputStream fos = new FileOutputStream(catalog);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(LG);
        oos.flush();
        oos.close();
        fos.close();
	}
	
	public static Boolean isAllowed(String url) throws IOException
	{
		URL urlObj = new URL(url);
		String path = urlObj.getPath();
		String u = urlObj.getProtocol() + "://" + urlObj.getHost() + "/robots.txt";
		System.out.println(u);
		URL main = new URL(u);
		BufferedReader in = new BufferedReader(new InputStreamReader(main.openStream()));
        String inputLine;
        while ((inputLine = in.readLine()) != null)
        {
        	if(inputLine.startsWith("A"))
        	{
        		if(inputLine.contains(path))
        		return true;
        	}
        }
        in.close(); 
        return false;
	}
	
	
	public static void writeToFile(String fileName, String title, String text, String raw, String docno, String charset) throws IOException {
		//initialize writers and buffers
		File file = new File("/home/sivaram/eclipse/files/AP_DATA/hw3/collection/"+fileName);
		
		FileUtils.write(file, "<DOC>\n"+"<HEAD>"+title+"</HEAD>\n"+"<URL>"+docno+"</URL>\n"+"<TEXT>\n"+text+"\n</TEXT>\n"+"<RAW>\n"+raw+"\n</RAW>\n"+"</DOC>\n", charset, true);
	}
}

