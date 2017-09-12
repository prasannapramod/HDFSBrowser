package com.example.HDFSBrowser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

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
public abstract class SnapShotHDFSBrowser extends AppDataSnapshotServerMap implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(SnapShotHDFSBrowser.class);
  
  @NotNull
  protected String directory;
  protected transient Configuration conf;
  protected transient FileSystem fs;
  protected int propCount;
  transient LoadingCache<String, FileContent> fileContents;
  String queryJSON;
  
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
    fileContents = CacheBuilder.<String, FileContent>newBuilder().maximumSize(2).build(createCacheLoader());
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
  
  private FileContent getFileContent(FileStatus fileStatus) throws ExecutionException, IOException
  {
    Path filePath = fileStatus.getPath();
    FileContent fileContent = fileContents.get(filePath.toString());
    if (fileContent.modificationTime > 0) {
      logger.info("wait 2 seconds");
      if ((System.currentTimeMillis() - fileContent.timelastread) >= 2000){
        logger.info("its been 2 seconds since last read");
        try (FSDataInputStream in = fs.open(filePath)) {
          in.seek(fileContent.filesize);
          readFileTillEOF(in, fileContent);
        }
        fileContent.modificationTime = fileStatus.getModificationTime();
        fileContent.filesize = fileStatus.getLen();
        fileContent.timelastread = System.currentTimeMillis();
      }
    } else {
      fileContent.modificationTime = fileStatus.getModificationTime();
      fileContent.filesize = fileStatus.getLen();
    }
    return fileContent;
  }
  
  private void readFileTillEOF(InputStream in, FileContent content) throws IOException
  {
    String line;
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
    while ((line = bufferedReader.readLine()) != null) {
      content.data.add(line);
    } 
  }
  
  private CacheLoader<String, FileContent> createCacheLoader()
  {
    return new CacheLoader<String, FileContent>()
    {
      @Override
      public FileContent load(String path) throws Exception
      {
        FileContent fileContent = new FileContent();
        fileContent.data = new CircularFifoBuffer(100);
        Path filePath = new Path(path);
        try (FSDataInputStream in = fs.open(filePath)) {
          readFileTillEOF(in, fileContent);
        }
        return fileContent;
      }
    };
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
  
  protected abstract FileStatus[] getDeviceFiles(String key) throws IOException;
  
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
      //String index = "0";//schemaKeys.get("index");
      int count = 50; //schemaKeys.get("count");
      
      List<GPOMutable> listLines = new ArrayList<>();

      try {
        FileStatus[] fileStatuses = operator.getDeviceFiles(key);
        fileLoop:
        for (FileStatus fileStatus : fileStatuses) {
          FileContent content = operator.getFileContent(fileStatus);
          for (String line : content.data) {
            map = new HashMap<>();
            i++;
            map.put("index", 51 - i + "");
            map.put("line", line);
            GPOMutable gpoRow = operator.convert(map);
            listLines.add(0, gpoRow);
            if (i >= count) break fileLoop;
          }
        }
      } catch (IOException | ExecutionException e) {
        logger.warn("Error processing files", e);
      }
      return new DataResultSnapshot(query, listLines, queueContext.getValue());
    }
  }
}
