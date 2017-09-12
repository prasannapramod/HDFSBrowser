package com.example.HDFSBrowser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Created by lakshmi on 8/3/17.
 */

public class HdfsBrowserOperator extends SnapShotHDFSBrowser
{
  private static final Logger logger = LoggerFactory.getLogger(SnapShotHDFSBrowser.class);

  @Override
  protected FileStatus[] getDeviceFiles(String deviceKey) throws IOException
  {
    FileStatus[] fileStatus = fs.listStatus(new Path(directory));

    Arrays.sort(fileStatus, new Comparator<FileStatus>()
    {
      @Override
      public int compare(FileStatus o1, FileStatus o2)
      {
        return Long.compare(o2.getModificationTime(), o1.getModificationTime());
      }
    });
    
    for (FileStatus fs : fileStatus) {
      logger.info("Path {}", fs.getPath().toString());
    }
    return fileStatus;
  }
}
