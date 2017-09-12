package com.example.HDFSBrowser;

import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;

/**
 * Created by lakshmi on 8/8/17.
 */
public class AppDataSnapshotServerMap extends AbstractAppDataSnapshotServer<Map<String, Object>>
{
  private Map<String, String> tableFieldToMapField;

  /**
   * Create operator.
   */
  public AppDataSnapshotServerMap()
  {
    //Do nothing
  }

  @Override
  public GPOMutable convert(Map<String, Object> inputEvent)
  {
    FieldsDescriptor fd = schema.getValuesDescriptor();
    GPOMutable values = new GPOMutable(fd);

    List<String> fields = fd.getFieldList();

    for (int index = 0; index < fields.size(); index++) {
      String field = fields.get(index);
      values.setFieldGeneric(field, inputEvent.get(getMapField(field)));
    }

    return values;
  }

  /**
   * This is a helper method which takes the name of a field in the schema and gets the
   * name of the corresponding key in an input map.
   * @param field The schema name of a field of interest.
   * @return The name of the corresponding Map key.
   */
  private String getMapField(String field)
  {
    if (tableFieldToMapField == null) {
      return field;
    }

    String mapField = tableFieldToMapField.get(field);

    if (mapField == null) {
      return field;
    }

    return mapField;
  }

  /**
   * Gets the tableFieldToMap map.
   * @return The tableFieldToMapField map.
   */
  public Map<String, String> getTableFieldToMapField()
  {
    return tableFieldToMapField;
  }

  /**
   * Sets the tableFieldToMapField map. This map is used for the case if the name of fields in
   * the map are different from the name of fields in the schema. In such a case you can specify
   * the mapping from fields in the schema to the corresponding key names in input maps.
   * If the name of a field in the schema is the same as the name of a key in the input maps,
   * then it is not necessary to specify anything in this map for that field.
   * @param tableFieldToMapField The tableFieldToMapField to set.
   */
  public void setTableFieldToMapField(@NotNull Map<String, String> tableFieldToMapField)
  {
    Preconditions.checkNotNull(tableFieldToMapField);

    for (Map.Entry<String, String> entry : tableFieldToMapField.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.tableFieldToMapField = Maps.newHashMap(tableFieldToMapField);
  }

  private static final Logger LOG = LoggerFactory.getLogger(com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap.class);
}
