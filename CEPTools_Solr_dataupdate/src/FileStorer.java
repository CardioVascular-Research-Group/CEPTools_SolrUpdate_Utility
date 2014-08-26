

import java.io.File;
import java.io.Serializable;


public class FileStorer implements Serializable{
	
	private String filename;
	private String filelocation;
	private String filetype;
	private String localfilestore;
	private String panel;
	private String figure;
	private String description;
	private String message;
	private File validator;
	private long filesize;
	private String figpandisplay;
	private static final long serialVersionUID = 4L;
	
	
	private int index;
	
	public FileStorer()
	{
		index = 0;
		filename = "";
		filetype = "";
		localfilestore = "";
		panel = description = figure = message =  figpandisplay = "";
		filesize = 0;
		validator = new File("validatorfile");
	
		
	}
	
	public String getFigpandisplay()
	{
		return figpandisplay;
	}
	
	public void setFigpandisplay(String f)
	{
		figpandisplay = f;
	}
	
	public String getMessage()
	{
		return message;
	}
	
	public void setMessage(String m)
	{
		message = m;
	}
	
	public File getValidator()
	{
	  return validator;	
	}
	
	public void setValidator(File v)
	{
		validator = v;
	}
	
	
	public long getFilesize()
	{
		return filesize;
	}
	
	public void setFilesize(long f)
	{
		filesize = f;
	}
	
    public String getPanel()
    {
    	return panel;
    }
    
    public String getFigure()
    {
    	return figure;
    }
    
    public String getDescription()
    {
    	return description;
    }
    
    public void setPanel(String p)
    {
    	panel = p;
    }
    
    public void setFigure(String f)
    {
    	figure = f;
    }
    
    public void setDescription(String d)
    {
    	description = d;
    }
	
	
    public int getIndex()
    {
    	return index;
    }
  
    public void setIndex(int i)
    {
    	this.index = i;
    }
      
    public String getLocalfilestore()
     {
    	return localfilestore;
    	  
     }
      
    public void setLocalfilestore(String lfs)
    {
    	this.localfilestore = lfs;
    }
      
      
	public void setFilename(String name)
	{
		this.filename = name;
	}
	
	public void setFilelocation(String location)
	{
		this.filelocation = location;
	}
	
	public String getFilename()
	{
		return filename;
	}
	
	public String getFilelocation()
	{
		return filelocation;
	}
	
	
	public String getFiletype()
	{
		return filetype;
	}
	
	public void setFiletype (String filetype)
	{
		this.filetype = filetype;
	}
	

}
