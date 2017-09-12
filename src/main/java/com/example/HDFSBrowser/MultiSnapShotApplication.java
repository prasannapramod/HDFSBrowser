/**
 * Put your copyright and license info here.
 */
package com.example.HDFSBrowser;

import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;

@ApplicationAnnotation(name = "MultiSnapShotApp")
public class MultiSnapShotApplication implements StreamingApplication
{
  public static final String SNAPSHOT_SCHEMA = "FileResult.json";
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    HdfsBrowserOperator fileReader01 = dag.addOperator("fileReader01", HdfsBrowserOperator.class);
    HdfsBrowserOperator fileReader02 = dag.addOperator("fileReader02", HdfsBrowserOperator.class);
    HdfsBrowserOperator fileReader03 = dag.addOperator("fileReader03", HdfsBrowserOperator.class);
    HdfsBrowserOperator fileReader04 = dag.addOperator("fileReader04", HdfsBrowserOperator.class);
    HdfsBrowserOperator fileReader05 = dag.addOperator("fileReader05", HdfsBrowserOperator.class);
    HdfsBrowserOperator fileReader06 = dag.addOperator("fileReader06", HdfsBrowserOperator.class);
    HdfsBrowserOperator fileReader07 = dag.addOperator("fileReader07", HdfsBrowserOperator.class);
    HdfsBrowserOperator fileReader08 = dag.addOperator("fileReader08", HdfsBrowserOperator.class);
    
    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    
    if (!StringUtils.isEmpty(gatewayAddress)) {        // added query support
      URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
      
      String snapshotServerJSON = SchemaUtils.jarResourceFileToString(SNAPSHOT_SCHEMA);
      fileReader01.setSnapshotSchemaJSON(snapshotServerJSON);
      fileReader02.setSnapshotSchemaJSON(snapshotServerJSON);
      fileReader03.setSnapshotSchemaJSON(snapshotServerJSON);
      fileReader04.setSnapshotSchemaJSON(snapshotServerJSON);
      fileReader05.setSnapshotSchemaJSON(snapshotServerJSON);
      fileReader06.setSnapshotSchemaJSON(snapshotServerJSON);
      fileReader07.setSnapshotSchemaJSON(snapshotServerJSON);
      fileReader08.setSnapshotSchemaJSON(snapshotServerJSON);
      
      PubSubWebSocketAppDataQuery wsQueryFile01 = new PubSubWebSocketAppDataQuery();
      wsQueryFile01.setUri(uri);
      wsQueryFile01.setTopic("HDFSBrowserOperatorFile01");
      fileReader01.setEmbeddableQueryInfoProvider(wsQueryFile01);

      PubSubWebSocketAppDataQuery wsQueryFile02 = new PubSubWebSocketAppDataQuery();
      wsQueryFile02.setUri(uri);
      wsQueryFile02.setTopic("HDFSBrowserOperatorFile02");
      fileReader02.setEmbeddableQueryInfoProvider(wsQueryFile02);

      PubSubWebSocketAppDataQuery wsQueryFile03 = new PubSubWebSocketAppDataQuery();
      wsQueryFile03.setUri(uri);
      wsQueryFile03.setTopic("HDFSBrowserOperatorFile03");
      fileReader03.setEmbeddableQueryInfoProvider(wsQueryFile03);

      PubSubWebSocketAppDataQuery wsQueryFile04 = new PubSubWebSocketAppDataQuery();
      wsQueryFile04.setUri(uri);
      wsQueryFile04.setTopic("HDFSBrowserOperatorFile04");
      fileReader04.setEmbeddableQueryInfoProvider(wsQueryFile04);

      PubSubWebSocketAppDataQuery wsQueryFile05 = new PubSubWebSocketAppDataQuery();
      wsQueryFile05.setUri(uri);
      wsQueryFile05.setTopic("HDFSBrowserOperatorFile05");
      fileReader05.setEmbeddableQueryInfoProvider(wsQueryFile05);

      PubSubWebSocketAppDataQuery wsQueryFile06 = new PubSubWebSocketAppDataQuery();
      wsQueryFile06.setUri(uri);
      wsQueryFile06.setTopic("HDFSBrowserOperatorFile06");
      fileReader06.setEmbeddableQueryInfoProvider(wsQueryFile06);

      PubSubWebSocketAppDataQuery wsQueryFile07 = new PubSubWebSocketAppDataQuery();
      wsQueryFile07.setUri(uri);
      wsQueryFile07.setTopic("HDFSBrowserOperatorFile07");
      fileReader07.setEmbeddableQueryInfoProvider(wsQueryFile07);

      PubSubWebSocketAppDataQuery wsQueryFile08 = new PubSubWebSocketAppDataQuery();
      wsQueryFile08.setUri(uri);
      wsQueryFile08.setTopic("HDFSBrowserOperatorFile08");
      fileReader08.setEmbeddableQueryInfoProvider(wsQueryFile08);
      
      PubSubWebSocketAppDataResult wsResultFile01 = dag.addOperator("wsResultFile01", new PubSubWebSocketAppDataResult());
      wsResultFile01.setUri(uri);
      wsResultFile01.setTopic("HDFSBrowserOperatorFileResult01");
      
      Operator.InputPort<String> queryResultFilePort01 = wsResultFile01.input;
      
      dag.addStream("Result File Lines 01", fileReader01.queryResult, queryResultFilePort01);

      PubSubWebSocketAppDataResult wsResultFile02 = dag.addOperator("wsResultFile02", new PubSubWebSocketAppDataResult());
      wsResultFile02.setUri(uri);
      wsResultFile02.setTopic("HDFSBrowserOperatorFileResult02");

      Operator.InputPort<String> queryResultFilePort02 = wsResultFile02.input;

      dag.addStream("Result File Lines 02", fileReader02.queryResult, queryResultFilePort02);

      PubSubWebSocketAppDataResult wsResultFile03 = dag.addOperator("wsResultFile03", new PubSubWebSocketAppDataResult());
      wsResultFile03.setUri(uri);
      wsResultFile03.setTopic("HDFSBrowserOperatorFileResult03");

      Operator.InputPort<String> queryResultFilePort03 = wsResultFile03.input;

      dag.addStream("Result File Lines 03", fileReader03.queryResult, queryResultFilePort03);

      PubSubWebSocketAppDataResult wsResultFile04 = dag.addOperator("wsResultFile04", new PubSubWebSocketAppDataResult());
      wsResultFile04.setUri(uri);
      wsResultFile04.setTopic("HDFSBrowserOperatorFileResult04");

      Operator.InputPort<String> queryResultFilePort04 = wsResultFile04.input;

      dag.addStream("Result File Lines 04", fileReader04.queryResult, queryResultFilePort04);

      PubSubWebSocketAppDataResult wsResultFile05 = dag.addOperator("wsResultFile05", new PubSubWebSocketAppDataResult());
      wsResultFile05.setUri(uri);
      wsResultFile05.setTopic("HDFSBrowserOperatorFileResult05");

      Operator.InputPort<String> queryResultFilePort05 = wsResultFile05.input;

      dag.addStream("Result File Lines 05", fileReader05.queryResult, queryResultFilePort05);

      PubSubWebSocketAppDataResult wsResultFile06 = dag.addOperator("wsResultFile06", new PubSubWebSocketAppDataResult());
      wsResultFile06.setUri(uri);
      wsResultFile06.setTopic("HDFSBrowserOperatorFileResult06");

      Operator.InputPort<String> queryResultFilePort06 = wsResultFile06.input;

      dag.addStream("Result File Lines 06", fileReader06.queryResult, queryResultFilePort06);

      PubSubWebSocketAppDataResult wsResultFile07 = dag.addOperator("wsResultFile07", new PubSubWebSocketAppDataResult());
      wsResultFile07.setUri(uri);
      wsResultFile07.setTopic("HDFSBrowserOperatorFileResult07");

      Operator.InputPort<String> queryResultFilePort07 = wsResultFile07.input;

      dag.addStream("Result File Lines 07", fileReader07.queryResult, queryResultFilePort07);

      PubSubWebSocketAppDataResult wsResultFile08 = dag.addOperator("wsResultFile08", new PubSubWebSocketAppDataResult());
      wsResultFile08.setUri(uri);
      wsResultFile08.setTopic("HDFSBrowserOperatorFileResult08");

      Operator.InputPort<String> queryResultFilePort08 = wsResultFile08.input;

      dag.addStream("Result File Lines 08", fileReader08.queryResult, queryResultFilePort08);
    }
  }
}
