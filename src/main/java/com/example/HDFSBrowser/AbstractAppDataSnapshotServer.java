package com.example.HDFSBrowser;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.experimental.AppData;
import com.datatorrent.lib.appdata.StoreUtils;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.AppDataWindowEndQueueManager;
import com.datatorrent.lib.appdata.query.QueryExecutor;
import com.datatorrent.lib.appdata.query.QueryManagerSynchronous;
import com.datatorrent.lib.appdata.query.serde.MessageDeserializerFactory;
import com.datatorrent.lib.appdata.query.serde.MessageSerializerFactory;
import com.datatorrent.lib.appdata.schemas.DataQuerySnapshot;
import com.datatorrent.lib.appdata.schemas.DataResultSnapshot;
import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.Query;
import com.datatorrent.lib.appdata.schemas.Result;
import com.datatorrent.lib.appdata.schemas.ResultFormatter;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.datatorrent.lib.appdata.schemas.SchemaRegistrySingle;
import com.datatorrent.lib.appdata.schemas.SchemaResult;
import com.datatorrent.lib.appdata.schemas.SnapshotSchema;

/**
 * Created by lakshmi on 8/8/17.
 */
public abstract class AbstractAppDataSnapshotServer<INPUT_EVENT> implements Operator, AppData.Store<String>
{
  /**
   * The {@link QueryManagerSynchronous} for the operator.
   */
  protected transient QueryManagerSynchronous<Query, Void, MutableLong, Result> queryProcessor;
  /**
   * The {@link MessageDeserializerFactory} for the operator.
   */
  private transient MessageDeserializerFactory queryDeserializerFactory;
  /**
   * The {@link MessageSerializerFactory} for the operator.
   */
  protected transient MessageSerializerFactory resultSerializerFactory;
  /**
   * The {@link SchemaRegistry} for the operator.
   */
  protected transient SchemaRegistry schemaRegistry;
  /**
   * The schema for the operator.
   */
  protected transient SnapshotSchema schema;

  @NotNull
  protected ResultFormatter resultFormatter = new ResultFormatter();
  protected String snapshotSchemaJSON;
  /**
   * The current data to be served by the operator.
   */
  protected List<GPOMutable> currentData = Lists.newArrayList();
  protected AppData.EmbeddableQueryInfoProvider<String> embeddableQueryInfoProvider;
  protected final transient ConcurrentLinkedQueue<SchemaResult> schemaQueue = new ConcurrentLinkedQueue<>();

  @AppData.ResultPort
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<>();

  /**
   * The queryExecutor execute the query and return the result.
   */
  protected QueryExecutor<Query, Void, MutableLong, Result> queryExecutor;

  private Set<String> tags;

  @AppData.QueryPort
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override
    public void process(String queryJSON)
    {
      processQuery(queryJSON);
    }
  };

  /**
   * process the query send.
   * provide this method to give sub class a chance to override.
   * @param queryJSON
   */
  protected void processQuery(String queryJSON)
  {
    LOG.debug("query {}", queryJSON);
    Message query = null;

    try {
      query = queryDeserializerFactory.deserialize(queryJSON);
    } catch (IOException ex) {
      LOG.error("Error parsing query: {}", queryJSON);
      LOG.error("{}", ex);
      return;
    }

    if (query instanceof SchemaQuery) {
      SchemaResult schemaResult = schemaRegistry.getSchemaResult((SchemaQuery)query);

      if (schemaResult != null) {
        LOG.debug("queueing {}", schemaResult);
        schemaQueue.add(schemaResult);
      }
    } else if (query instanceof DataQuerySnapshot) {
      queryProcessor.enqueue((DataQuerySnapshot)query, null, null);
    }
  }
  
  @InputPortFieldAnnotation(optional = true) 
  public final transient DefaultInputPort<List<INPUT_EVENT>> input = new DefaultInputPort<List<INPUT_EVENT>>()
  {
    @Override
    public void process(List<INPUT_EVENT> rows)
    {
      processData(rows);
    }
  };

  protected void processData(List<INPUT_EVENT> rows)
  {
    currentData.clear();

    for (INPUT_EVENT inputEvent : rows) {
      GPOMutable gpoRow = convert(inputEvent);
      currentData.add(gpoRow);
    }
  }

  /**
   * Create operator.
   */
  public AbstractAppDataSnapshotServer()
  {
    //Do nothing
  }

  /**
   * This method converts input data to GPOMutable objects to serve.
   * @param inputEvent The input object to convert to a {@link GPOMutable{.
   * @return The converted input event.
   */
  public abstract GPOMutable convert(INPUT_EVENT inputEvent);


  @Override
  public final void activate(Context.OperatorContext ctx)
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.activate(ctx);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setup(Context.OperatorContext context)
  {
    setupSchema();

    schemaRegistry = new SchemaRegistrySingle(schema);
    //Setup for query processing
    setupQueryProcessor();

    queryDeserializerFactory = new MessageDeserializerFactory(SchemaQuery.class,
      DataQuerySnapshot.class);
    queryDeserializerFactory.setContext(DataQuerySnapshot.class, schemaRegistry);
    resultSerializerFactory = new MessageSerializerFactory(resultFormatter);
    queryProcessor.setup(context);

    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.enableEmbeddedMode();
      LOG.info("An embeddable query operator is being used of class {}.", embeddableQueryInfoProvider.getClass().getName());
      StoreUtils.attachOutputPortToInputPort(embeddableQueryInfoProvider.getOutputPort(), query);
      embeddableQueryInfoProvider.setup(context);
    }
  }

  protected void setupSchema()
  {
    schema = new SnapshotSchema(snapshotSchemaJSON);
    if (tags != null && !tags.isEmpty()) {
      schema.setTags(tags);
    }
  }

  protected void setupQueryProcessor()
  {
    queryProcessor = QueryManagerSynchronous.newInstance(queryExecutor == null ? new AbstractAppDataSnapshotServer.SnapshotComputer() : queryExecutor,
      new AppDataWindowEndQueueManager<Query, Void>());
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.beginWindow(windowId);
    }

    queryProcessor.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.endWindow();
    }

    {
      Result result;

      while ((result = queryProcessor.process()) != null) {
        String resultJSON = resultSerializerFactory.serialize(result);
        LOG.debug("emitting {}", resultJSON);
        queryResult.emit(resultJSON);
      }
    }

    {
      SchemaResult schemaResult;

      while ((schemaResult = schemaQueue.poll()) != null) {
        String schemaResultJSON = resultSerializerFactory.serialize(schemaResult);
        LOG.debug("emitting {}", schemaResultJSON);
        queryResult.emit(schemaResultJSON);
      }
    }

    queryProcessor.endWindow();
  }

  @Override
  public void teardown()
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.teardown();
    }

    queryProcessor.teardown();
  }

  @Override
  public void deactivate()
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.deactivate();
    }
  }

  /**
   * Gets the JSON for the schema.
   * @return the JSON for the schema.
   */
  public String getSnapshotSchemaJSON()
  {
    return snapshotSchemaJSON;
  }

  /**
   * Sets the JSON for the schema.
   * @param snapshotSchemaJSON The JSON for the schema.
   */
  public void setSnapshotSchemaJSON(String snapshotSchemaJSON)
  {
    this.snapshotSchemaJSON = snapshotSchemaJSON;
  }

  /**
   * Gets the {@link ResultFormatter} for the data.
   * @return The {@link ResultFormatter} for the data.
   */
  public ResultFormatter getResultFormatter()
  {
    return resultFormatter;
  }

  /**
   * Sets the {@link ResultFormatter} for the data.
   * @param resultFormatter The {@link ResultFormatter} for the data.
   */
  public void setResultFormatter(ResultFormatter resultFormatter)
  {
    this.resultFormatter = resultFormatter;
  }

  @Override
  public AppData.EmbeddableQueryInfoProvider<String> getEmbeddableQueryInfoProvider()
  {
    return embeddableQueryInfoProvider;
  }

  @Override
  public void setEmbeddableQueryInfoProvider(AppData.EmbeddableQueryInfoProvider<String> embeddableQueryInfoProvider)
  {
    this.embeddableQueryInfoProvider = Preconditions.checkNotNull(embeddableQueryInfoProvider);
  }

  /**
   * The {@link QueryExecutor} which returns the results for queries.
   */
  public class SnapshotComputer implements QueryExecutor<Query, Void, MutableLong, Result>
  {
    @Override
    public Result executeQuery(Query query, Void metaQuery, MutableLong queueContext)
    {
      return new DataResultSnapshot(query,
        currentData,
        queueContext.getValue());
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(com.datatorrent.lib.appdata.snapshot.AbstractAppDataSnapshotServer.class);

  public QueryExecutor<Query, Void, MutableLong, Result> getQueryExecutor()
  {
    return queryExecutor;
  }

  public void setQueryExecutor(QueryExecutor<Query, Void, MutableLong, Result> queryExecutor)
  {
    this.queryExecutor = queryExecutor;
  }

  public List<GPOMutable> getCurrentData()
  {
    return currentData;
  }

  public Set<String> getTags()
  {
    return tags;
  }

  public void setTags(Set<String> tags)
  {
    this.tags = tags;
  }
}
