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
  protected String getDeviceFileName(String deviceKey)
  {
    FileStatus[] fileStatus = null;
    String fileName = null;

    try {
      fileStatus = fs.listStatus(new Path(directory));
      
      if (fileStatus.length != 0) {
        Arrays.sort(fileStatus, new Comparator<FileStatus>()
        {
          @Override
          public int compare(FileStatus o1, FileStatus o2)
          {
            return Long.compare(o1.getModificationTime(), o2.getModificationTime());
          }
        });

        fileName = fileStatus[fileStatus.length - 1].getPath().getName();
        logger.info("FileName is {}", fileName);
        
      } else {
        fileName = null;
        logger.info("No File present");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return fileName;
  }
}
