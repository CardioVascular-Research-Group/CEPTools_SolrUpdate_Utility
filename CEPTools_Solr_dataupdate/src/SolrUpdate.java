/*Copyright 2013 Johns Hopkins University Institute for Computational Medicine

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
/**
* @author Shallon Brown 2014
* 
*/
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;




public class SolrUpdate {
	
	
	private List<Publication> solrrecords;
	private List<Publication> publications;
	private List<String> filesanddata;
	private SolrInputDocument metadoc = new SolrInputDocument();
	private HttpSolrServer server = new HttpSolrServer("http://localhost:8983/solr");
	private Publication selectedPub;
	private List<FileStorer> allfiles;
	private boolean sendback;
	
	   
	 public static void main (String [] args)
	 {
		 SolrUpdate mysolr = new SolrUpdate();
		 mysolr.getAllSolrRecords();
		 mysolr.updateRecords();
		 if(mysolr.getSendback() == true)
		 {
			for(Publication currpub: mysolr.getSolrrecords())
			 {
		     System.out.println("Returning the updated record to SOLR: " + currpub.getPmid());
		    mysolr.setSelectedPub(currpub);
			mysolr.sendUpdatestosolr(); 
			 }
			System.out.println("All records now up to date within SOLR. Process now complete.");
		 }
		 else
		 {
			 System.out.println("No records to update. Process now complete.");
		 }
	 }
	   
	   
	 public boolean getSendback()
	 {
		 return sendback;
	 }
	 
	 public List<Publication> getSolrrecords()
	 {
		 return solrrecords;
	 }
	 
	 public Publication getSelectedPub()
	 {
		 return selectedPub;
	 }
	 public void setSelectedPub(Publication p)
	 {
		 selectedPub = p;
	 }
	public SolrUpdate()
	{
		solrrecords = new ArrayList<Publication>();
		publications = new ArrayList<Publication>();
		filesanddata= new ArrayList<String> ();
		allfiles = new ArrayList<FileStorer> ();
		sendback = false;

	}
	
	public void getAllSolrRecords()
	{
		String pmid;
		 try
		 {
			
			 CoreAdminRequest adminRequest = new CoreAdminRequest();
			 adminRequest.setAction(CoreAdminAction.RELOAD);

			 SolrServer solr = new HttpSolrServer ("http://localhost:8983/solr");

			 String query;

			 query = "pmid:*";
			 
			
			 SolrQuery theq = new SolrQuery();
			 theq.setQuery(query);
			 theq.setStart(0);
			 theq.setRows(10000);
			 
			 QueryResponse response = new QueryResponse();

			 response = solr.query(theq);

			 SolrDocumentList list = response.getResults();
		

			 int docnum = 1;
			 for(SolrDocument doc: list)
			 {
				Publication currlist = new Publication();
				
				 List<String> fullnames =  new ArrayList<String> ();
				 String currepubsum1 = "", currepubsum2 = "";
				
				 if(doc.getFieldValue("abstract")!=null)
				 {
				 currlist.setAbstract(doc.getFieldValue("abstract").toString()); 
				 }
				 if(doc.getFieldValue("ptitle")!=null)
				 {
				 currlist.setTitle(doc.getFieldValue("ptitle").toString());
				 }
				 if(doc.getFieldValue("author_fullname_list")!=null)
				 {
	              currlist.setFirst5authors(doc.getFieldValue("author_fullname_list").toString());
	              }
	              if(doc.getFieldValue("pmid")!=null)
	              {
	              currlist.setPmid(Integer.valueOf(doc.getFieldValue("pmid").toString()));
	              }
	              if(doc.getFieldValue("completion")!=null)
	              {
	              currlist.setCompletion(Boolean.valueOf(doc.getFieldValue("completion").toString()));
	              }
	              if(doc.getFieldValue("lruid")!=null)
	              {
	              currlist.setLruid(doc.getFieldValue("lruid").toString());
	              }
	              if(doc.getFieldValue("draftpoint")!=null)
	              {
	              currlist.setDraftpoint(Integer.valueOf(doc.getFieldValue("draftpoint").toString()));
	              }

				if(doc.getFieldValue("journalname")!=null)
				{
				currlist.setJournalname(doc.getFieldValue("journalname").toString());
				}
				
				if(doc.getFieldValue("journalyear")!=null)
				{
				currlist.setJournalyear(doc.getFieldValue("journalyear").toString());
				}
				if(doc.getFieldValue("journalday")!=null)
				{
				currlist.setJournalday(doc.getFieldValue("journalday").toString());
				}
				if(doc.getFieldValue("journalmonth")!=null)
				{
				currlist.setJournalmonth(doc.getFieldValue("journalmonth").toString());
				}
				if(doc.getFieldValue("journalpage")!=null)
				{
				currlist.setJournalstartpg(doc.getFieldValue("journalpage").toString());
				}
				if(doc.getFieldValue("journalissue")!=null)
				{
				currlist.setJournalissue(doc.getFieldValue("journalissue").toString());
				}
				if(doc.getFieldValue("journalvolume")!=null)
				{
				currlist.setJournalvolume(doc.getFieldValue("journalvolume").toString());
				}
				if(doc.getFieldValue("publicationdate_year")!=null)
				{
				currlist.setYear(doc.getFieldValue("publicationdate_year").toString());
				}
				if(doc.getFieldValue("doi") != null)
				{
				currlist.setDoi(doc.getFieldValue("doi").toString());
				}
				
				if(doc.getFieldValues("pfileinfo") != null)
				{
				
					Collection<Object> currcoll = doc.getFieldValues("pfileinfo");
					for(Object currobj: currcoll)
					{
						currlist.getFilesanddata().add(currobj.toString());
					}
					

				}
				if(doc.getFieldValue("author_firstname") != null)
				{
				currlist.setFauthors((List<String>) doc.getFieldValue("author_firstname"));
				}
				if(doc.getFieldValue("author_lastname") != null)
				{
				currlist.setLauthors((List<String>) doc.getFieldValue("author_lastname"));
				}
				
				if(doc.getFieldValue("epubmonth") != null)
				{
				currlist.setEpubmonth(doc.getFieldValue("epubmonth").toString());
				}
				
				if(doc.getFieldValue("epubyear") != null)
				{
				currlist.setEpubyear(doc.getFieldValue("epubyear").toString());
				}
				
				if(doc.getFieldValue("epubday") !=null)
				{
				currlist.setEpubday(doc.getFieldValue("epubday").toString());
				}
				
			
			
				int counter = 0;
	
				
				for(String currstring: currlist.getFauthors())
				{
				    currstring += " " + currlist.getLauthors().get(counter); 
				    fullnames.add(currstring);
					counter++;
				}
				
				currlist.setFullnames(fullnames);
				
				if(currlist.getJournalvolume().length()>0)
 	        	{
 	        		currepubsum2 +=  currlist.getJournalvolume();
 	        	}
 	        	
 	        	if(currlist.getJournalissue().length()>0)
 	        	{
 	        		currepubsum2 += "("+ currlist.getJournalissue() + ")"+ ":";
 	        	}
 	        	
 	        	if(currlist.getJournalstartpg().length()>0)
 	        	{
 	        		currepubsum2 += currlist.getJournalstartpg() + ".";
 	        	}

	              
 	        	if( currlist.getEpubday().length()<1 && currlist.getEpubmonth().length()<1  && currlist.getEpubyear().length()<1)
 	        	{
 	        		currepubsum1 = "[Epub ahead of print]"; 
           	 }
 	        	else if(currlist.getEpubyear().length()>0)
 	        	{
 	        		currepubsum1= "Epub "  + currlist.getEpubyear() + " " + currlist.getEpubmonth() + " " + currlist.getEpubday();
 	        	}
 	        	else
 	        	{
 	        		currepubsum1 = "";
 	        	}
				
	              currlist.setEpubsum(currepubsum1);
	              currlist.setEpubsum2(currepubsum2);
	              currlist.setIndex(docnum);

	            
				
				if(currlist.getCompletion() == false)
				{
					currlist.setComp("Hidden");
				}
				else
				{
					currlist.setComp("Visible");
				}

				solrrecords.add(currlist);
				docnum++;
			 }

		 }
		 catch (Exception ex)
		 {
			System.out.println(ex);

		 }
		  

	  System.out.println("There are a total of this many records gathered: " + solrrecords.size());
	}
	
	public void updateRecords()
	{
		int pmidrecordschanged = 0;
		int totalchanges = 0;
		int loopcount = 0;
		
		for(Publication currpub: solrrecords)
		{
			
	      // Publication currpubmedrecord = solrrecords.
			Iterator solriter = solrrecords.iterator();
			int currpmid = currpub.getPmid();
			boolean found = false;
			Publication currsolrrecord = null;
			boolean recordupdated = false;
			
			
			while(solriter.hasNext() && found == false)
			{
				currsolrrecord = (Publication) solriter.next();
				if(currsolrrecord.getPmid() == currpmid)
				{
					found = true;
				}
			}
			
			
			if(currsolrrecord!= null && found == true)
			{
				loopcount++;
				//check each variable from SOLR and update if needed
				//System.out.println("record loops: " + loopcount);
				
				if(!currsolrrecord.getAbstract().equals(currpub.getAbstract()))
				{
					currpub.setAbstract(currsolrrecord.getAbstract());
					System.out.println("Abstract for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getTitle().equals(currpub.getTitle()))
				{
					currpub.setTitle(currsolrrecord.getTitle());
					System.out.println("Title for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getYear().equals(currpub.getYear()))
				{
					currpub.setYear(currsolrrecord.getYear());
					System.out.println("Year for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getDoi().equals(currpub.getDoi()))
				{
					currpub.setDoi(currsolrrecord.getDoi());
					System.out.println("Doi for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getJournalvolume().equals(currpub.getJournalvolume()))
				{
					currpub.setJournalvolume(currsolrrecord.getJournalvolume());
					System.out.println("Volume for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getJournalissue().equals(currpub.getJournalissue()))
				{
					currpub.setJournalissue(currsolrrecord.getJournalissue());
					System.out.println("Issue for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getJournalday().equals(currpub.getJournalday()))
				{
					currpub.setJournalday(currsolrrecord.getJournalday());
					System.out.println("Journal Day for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getJournalmonth().equals(currpub.getJournalmonth()))
				{
					currpub.setJournalmonth(currsolrrecord.getJournalmonth());
					System.out.println("Journal Month for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getJournalyear().equals(currpub.getJournalyear()))
				{
					currpub.setJournalyear(currsolrrecord.getJournalyear());
					System.out.println("Journal Year for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getJournalname().equals(currpub.getJournalname()))
				{
					currpub.setJournalname(currsolrrecord.getJournalname());
					System.out.println("Journal Name for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getJournalstartpg().equals(currpub.getJournalstartpg()))
				{
					currpub.setJournalstartpg(currsolrrecord.getJournalstartpg());
					System.out.println("Journal startpage for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getEpubday().equals(currpub.getEpubday()))
				{
					currpub.setEpubday(currsolrrecord.getEpubday());
					System.out.println("Epub Day for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getEpubyear().equals(currpub.getEpubyear()))
				{
					currpub.setEpubyear(currsolrrecord.getEpubyear());
					System.out.println("Epub Year for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				if(!currsolrrecord.getEpubmonth().equals(currpub.getEpubmonth()))
				{
					currpub.setEpubmonth(currsolrrecord.getEpubmonth());
					System.out.println("Epub Month for PMID "+ currpub.getPmid() + " Updated!");
					totalchanges ++;
					recordupdated = true;
				}
				
				if(recordupdated == true)
				{
					pmidrecordschanged ++;
				}
			}
			
			
		}
		
		System.out.println("A total of " + totalchanges +" changes were made across " + pmidrecordschanged + " records.");
		//set a variable to SOLR can be properly updated.
		if((totalchanges + pmidrecordschanged)>0)
		{
			sendback = true;
		}
	}
	
	
public void sendUpdatestosolr()
{
	  


		try
		{
			setSOLRMetadata();
		    server.add(metadoc);
		    server.commit();
		}
		catch(Exception ex)
		{
			System.out.println(ex);
		}
		
		
}
	


	public void setSOLRMetadata()
	{ 
			 server = new HttpSolrServer("http://localhost:8983/solr");
			 
			
			 metadoc = new SolrInputDocument();
		 	 metadoc.addField("pmid", selectedPub.getPmid());

		 	 metadoc.addField("abstract", selectedPub.getAbstract());
		 	 metadoc.addField("publicationdate_year", selectedPub.getYear());
		 	 metadoc.addField("doi", selectedPub.getDoi());
			 metadoc.addField("journalvolume", selectedPub.getJournalvolume());
	    	 metadoc.addField("journalissue", selectedPub.getJournalissue());
	    	 metadoc.addField("journalmonth", selectedPub.getJournalmonth());
	    	 metadoc.addField("journalyear", selectedPub.getJournalyear());
	    	 metadoc.addField("journalday", selectedPub.getJournalday());
	    	 metadoc.addField("journalname", selectedPub.getJournalname());
	    	 metadoc.addField("journalpage", selectedPub.getJournalstartpg());
	    	 metadoc.addField("epubday", selectedPub.getEpubday());
	    	 metadoc.addField("epubmonth", selectedPub.getEpubmonth());
	    	 metadoc.addField("epubyear", selectedPub.getEpubyear());
	    	 metadoc.addField("author_fullname_list", selectedPub.getAuthorfull());
	    	 metadoc.addField("completion", selectedPub.getCompletion());
	    	 metadoc.addField("draftpoint", selectedPub.getDraftpoint() ); 
	    
	    	 metadoc.addField("lruid",  selectedPub.getLruid());
	    	 metadoc.addField("ptitle", selectedPub.getTitle() );  
		   
		  for(int i=0; i<selectedPub.getFauthors().size(); i++) 
		  {
		     metadoc.addField("author_firstname",selectedPub.getFauthors().get(i));
		     metadoc.addField("author_lastname",selectedPub.getLauthors().get(i)); 
		  }
		  
		 for(String currstring: selectedPub.getFilesanddata())
		 {
			  metadoc.addField("pfileinfo", currstring);
		 }
	
		    
	}
	
	
	//Utility Methods
	
	public void processUrl() throws Exception
	{
	   
	    	String jv,jn,ji,jd,jm,jy,jsp,authorfull, doi, epday, epmonth, epyear, epubsum, epubsum2 = "";
	    	jv=jn=ji=jd=jm=jy=jsp=authorfull=doi=epday=epmonth=epyear =epubsum = epubsum2 = "";
	    	
	    	SAXReader reader = new SAXReader();
	    	SAXReader reader2 = new SAXReader();
		    Document document = null;

	         String mytitle, myabstract, myyear, myfullname = "";
	         Element journalname, journalyear, journalmonth, journalday, journalvolume, journalissue, journalpagestart, epubday, epubmonth, epubyear, pubdoi;
	         int mypmid;
	         
	         
	         List<String> mylauthors = new ArrayList<String>();
	         List<String> myfauthors = new ArrayList<String>();
	         List<String> myfnames = new ArrayList<String>();
	         
	         //PubMed

	             String pubmedlist = "";
	             Iterator iditer = publications.iterator();
	            
	               
	                   while(iditer.hasNext())
	                   {
	                       int currpmid =  ((Publication) iditer.next()).getPmid();
	                       if(pubmedlist.length() < 1)
	                       {
	                       pubmedlist += currpmid ;
	                     
	                       }
	                       else
	                       {
	                           pubmedlist += "," + currpmid; 
	                       }
	                   }
	                  
	             
	              String url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id="+pubmedlist+"&retmax=200&retmode=xml&rettype=abstract";   
	                Document pubdoc = reader2.read(url);   
	            

	  	           @SuppressWarnings("unchecked")
	  	           List<Node> thelist = pubdoc.selectNodes("//PubmedArticle| //PubmedBookArticle");

	  	           Element abstractnode, titlenode, yearsnode, pmidnode;
	  	           @SuppressWarnings("rawtypes")
	  	           List firstnamenode;
	  	           @SuppressWarnings("rawtypes")
	  	           List lastnamenode;
	  	          
	  	           
	  	          for (Node currnode: thelist)
	  	          {
	  	           mylauthors = new ArrayList<String>();
	  		       myfauthors = new ArrayList<String>();
	  		       myfnames = new ArrayList<String>();
	  	        	  epubsum = epubsum2 = authorfull =  "";
	  	
  	              titlenode= (Element) currnode.selectSingleNode(".//ArticleTitle | .//BookTitle");
  	              yearsnode= (Element) currnode.selectSingleNode(".//PubDate/Year | .//DateCompleted/Year | .//DateCreated/Year");
  	              journalname =(Element) currnode.selectSingleNode(".//Journal/Title");
  	              journalyear =(Element) currnode.selectSingleNode(".//PubDate/Year"); 
  	              journalmonth =(Element) currnode.selectSingleNode(".//PubDate/Month"); 
  	          
  	              journalday =(Element) currnode.selectSingleNode(".//PubDate/Day"); 
  	              journalvolume =(Element) currnode.selectSingleNode(".//JournalIssue/Volume"); 
  	              journalissue =(Element) currnode.selectSingleNode(".//JournalIssue/Issue"); 
  	              journalpagestart =(Element) currnode.selectSingleNode(".//Pagination/MedlinePgn");
	  	         
	  	            
	  	          epubday = (Element) currnode.selectSingleNode(".//PubMedPubDate[@PubStatus='aheadofprint']/Day  | .//PubMedPubDate[@PubStatus='epublish']/Day "); 
	              epubmonth = (Element) currnode.selectSingleNode(".//PubMedPubDate[@PubStatus='aheadofprint']/Month | .//PubMedPubDate[@PubStatus='epublish']/Month");
	              epubyear =(Element) currnode.selectSingleNode(".//PubMedPubDate[@PubStatus='aheadofprint']/Year | .//PubMedPubDate[@PubStatus='epublish']/Year");
	              
	              
	              pubdoi  =(Element) currnode.selectSingleNode(".//ArticleId[@IdType='doi']"); 

  	              firstnamenode= currnode.selectNodes(".//ForeName");
  	              lastnamenode=  currnode.selectNodes(".//LastName");
  	              abstractnode = (Element) currnode.selectSingleNode(".//Abstract/AbstractText[1]");
  	              pmidnode = (Element) currnode.selectSingleNode(".//PMID");
  	              
  	              myfnames = new ArrayList<String>();
  	              @SuppressWarnings("rawtypes")
				  Iterator fiter = firstnamenode.iterator();
  	              @SuppressWarnings("rawtypes")
				  Iterator liter = lastnamenode.iterator();
	  	           
	  	              if(journalname !=null)
	  	              {
	  	            	  jn = journalname.getText();
	  	              }
	  	              if(journalvolume!=null)
	  	              {
	  	            	  jv = journalvolume.getText();
	  	              }
	  	              if(journalissue!=null)
	  	              {
	  	            	  ji = journalissue.getText();
	  	              }
	  	              if(journalmonth!=null)
	  	              {
	  	            	  jm = journalmonth.getText();
	  	              }
	  	              if(journalyear!=null)
	  	              {
	  	            	  jy = journalyear.getText();
	  	              }
	  	              if(journalpagestart!=null)
	  	              {
	  	            	  jsp =journalpagestart.getText();
	  	        	  }
	  	              if(journalday != null)
	  	              {
	  	            	  jd = journalday.getText();
	  	              }
	  	              if(epubday != null)
	  	              {
	  	            	  epday = epubday.getText();
	  	              }	
	  	              if(epubmonth != null)
	  	              {
	  	            	  epmonth = epubmonth.getText();
	  	              }
	  	              if(epubyear != null)
	  	              {
	  	            	  epyear = epubyear.getText();
	  	              }
	  	              if (pubdoi != null)
	  	              {
	  	            	  doi = "doi: " + pubdoi.getText();
	  	              }
	  	              if(jv.length()>0)
	  	              {
	  	            	  epubsum2 +=  jv ;
	  	              }
	  	        	
	  	              if(ji.length()>0)
	  	              {
	  	            	  epubsum2 += "("+ ji + ")"+ ":";
	  	              }
	  	        	
	  	              if(jsp.length()>0)
	  	              {
	  	            	  epubsum2 += jsp + ".";
	  	              }

		              if( epmonth.length()<1 && epyear.length()<1  && epday.length()<1)
	  	              {	
	  	            	epubsum = "[Epub ahead of print]"; 
	  	              }
		              else if(epyear.length()>0)
		              {
		            	epubsum = "Epub "  + epyear + " " + epmonth + " " + epday;
		              }
		              else
		              {
		            	epubsum = "";
		              }
	  	  
	  	              mytitle = titlenode.getText();
	  	              myyear = yearsnode.getText();
	  	              mypmid = Integer.valueOf(pmidnode.getText());
	  	           
	  	                
	  	                while(fiter.hasNext())
	  	                {
	  	                 Element fname =  (Element) fiter.next();   
	  	                 Element lname =  (Element) liter.next();  
	  	                 
	  	                 myfauthors.add(fname.getText());
	  	             	 mylauthors.add(lname.getText());
	  	               
	  	             	 myfullname = fname.getText() + " " + lname.getText();
		                 myfnames.add(myfullname);
		                
				                if(fiter.hasNext())
			            		{
				                	authorfull = authorfull + myfullname + ", ";
			            		}
				                else
				                {
				                	authorfull = authorfull + myfullname ;
				                }
	  	                 
	  	                }
	  	                
	  	              
	  	              if(abstractnode != null)
	  	              {   
	  	            	myabstract = abstractnode.getText();
	  	              }
	  	              else
	  	              {
	  	            	  myabstract = "NO ABSTRACT FOUND.";
	  	              }    

	  	              publications.add(new Publication(mytitle, myabstract, myyear, myfauthors, mylauthors, myfnames, jv, jn,jy, jm, jd, jsp, ji, epday, epmonth, epyear, doi, epubsum, epubsum2, authorfull, mypmid));

	  	           
	  	        
	  	          }  
	  	         
	  	       
	             
	           }
	

	         
	     
	

	

}
