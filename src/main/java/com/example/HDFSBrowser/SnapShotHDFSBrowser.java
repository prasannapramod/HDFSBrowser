package com.example.HDFSBrowser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Throwables;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.QueryExecutor;
import com.datatorrent.lib.appdata.schemas.DataResultSnapshot;
import com.datatorrent.lib.appdata.schemas.Query;
import com.datatorrent.lib.appdata.schemas.Result;

/**
 * Created by lakshmi on 7/21/17.
 */
//public abstract class SnapShotHDFSBrowser extends BaseOperator implements InputOperator, AppData.Store<String> 
public abstract class SnapShotHDFSBrowser extends AppDataSnapshotServerMap implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(SnapShotHDFSBrowser.class);
  
  @NotNull
  protected String multiDirectory;
  private String directory;
  protected transient Configuration conf;
  protected transient FileSystem fs;
  protected int propCount;
  transient Map<String, FileContent> fileContents = new HashMap<>();
  String queryJSON;
  String currentFile = null;
  long currentFileSize = 0;
  String previousFile = null;
  
  public String getdirectory()
  {
    return directory;
  }

  public void setdirectory(String directory)
  {
    this.directory = directory;
  }

  public int getPropCount()
  {
    return propCount;
  }

  public void setPropCount(int propCount)
  {
    this.propCount = propCount;
  }

  public String getQueryJSON()
  {
    return queryJSON;
  }

  public void setQueryJSON(String queryJSON)
  {
    this.queryJSON = queryJSON;
  }

  public SnapShotHDFSBrowser()
  {
    queryExecutor = new CustomQueryExecutor(this);
  }

  public String getMultiDirectory()
  {
    return multiDirectory;
  }

  public void setMultiDirectory(String multiDirectory)
  {
    this.multiDirectory = multiDirectory;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    conf = new Configuration();
    
    try {
      fs = FileSystem.get(URI.create(directory.toString()), conf);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    try {
      fs.close();
    } catch (IOException e) {
      logger.warn("Error during filesystem close.", e);
    }
  }
  
  public List<String> getInfo(String key, String fileName, int index, int count) throws IOException
  {
    List<String> info = new ArrayList<>();
    int numberParts = getNumberParts(fileName);
    boolean done = false;
    int skip = 0;
    while ((numberParts > 0) && !done) {
      String partFileName = getPartFileName(fileName, numberParts--);
      List<String> contents = getFileContents(partFileName);
      ListIterator<String> iterator = contents.listIterator(contents.size());
      logger.info("line and fileName is {}", fileName);
      while (iterator.hasPrevious() && !done) {
        String line = iterator.previous();
        if (doesLineMatchKey(line, key) && (skip++ >= index)) {
          //logger.info("line is {}", line);
          info.add(line);
          if (info.size() >= count) {
            done = true;
          }
        }
      }
    }
    return info;
  }
  
  protected boolean doesLineMatchKey(String line, String key)
  {
    // TODO Can be overriden by child to match their condition
    if (key.equalsIgnoreCase("All")) {
      return true;
    } else {
      return line.contains(key);
    }
  }
  
  protected List<String> getFileContents(String fileName) throws IOException
  {
    currentFile = fileName;
    FileContent contents = fileContents.get(fileName);
    Path filePath = new Path(directory, fileName);
    String line;
    
    try (FSDataInputStream in = fs.open(filePath)) {
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
      
      if (contents == null) {
        contents = new FileContent();

        while ((line = bufferedReader.readLine()) != null) {
          contents.lines.add(line);
          contents.filesize = contents.filesize + ((long)line.getBytes().length + 1);   //check here
        }
      } else {
        in.seek(contents.filesize);

        while ((line = bufferedReader.readLine()) != null) {
          contents.lines.add(line);
          contents.filesize = contents.filesize + ((long)line.getBytes().length + 1);   //check here
        }
      }
    }
    
    return contents.lines;
  }
  
  protected int getNumberParts(String fileName)
  {
    // TODO Return number of parts
    return 1;
  }

  protected String getPartFileName(String fileName, int partNumber)
  {
    // TODO return correct part filename
    return fileName;
  }
  
  protected abstract String getDeviceFileName(String key);
  
  @Override
  public void emitTuples()
  {
    
  } 

  public static class CustomQueryExecutor implements QueryExecutor<Query, Void, MutableLong, Result>
  {
    private SnapShotHDFSBrowser operator;

    public CustomQueryExecutor(SnapShotHDFSBrowser operator)
    {
      this.operator = operator;
    }

    public CustomQueryExecutor()
    {
    }

    @Override
    public Result executeQuery(Query query, Void metaQuery, MutableLong queueContext)
    {
      Map<String, String> schemaKeys = query.getSchemaKeys();
      Map<String, Object> map;
      List<Map<String,Object>> snapList = new ArrayList<>();
      int i = 0;

      String key = "All";
      String index = "0";//schemaKeys.get("index");
      String count = "10"; //schemaKeys.get("count");
      
      if (key == null) {
        key = "All";
      }
      
      String fileName = operator.getDeviceFileName(key);
      logger.info("Now reading file is {}", fileName);

      if (fileName != null) {
        List<String> contents = null;
        try {
          contents = operator.getInfo(key, fileName, Integer.parseInt(index), Integer.parseInt(count));
          for (String line : contents) {
            map = new HashMap<>();
            i++;
            map.put("index",i + "");
            map.put("line", line);
            snapList.add(map);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else {
        logger.info("filename is null");
      }
      
      List<GPOMutable> listLines = new ArrayList<>();
      
      for (Map<String, Object> inputEvent : snapList) {
        GPOMutable gpoRow = operator.convert(inputEvent);
        listLines.add(gpoRow);
      }
      
      return new DataResultSnapshot(query, listLines, queueContext.getValue());
    }
  }
}
