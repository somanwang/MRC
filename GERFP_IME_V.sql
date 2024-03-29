CREATE VIEW GERFP_IME_V AS
SELECT vv."ROW_ID",
       vv."FLEX_VALUE_SET_ID",
       vv."FLEX_VALUE_ID",
       vv."FLEX_VALUE",
       vv."LAST_UPDATE_DATE",
       vv."LAST_UPDATED_BY",
       vv."CREATION_DATE",
       vv."CREATED_BY",
       vv."LAST_UPDATE_LOGIN",
       vv."ENABLED_FLAG",
       vv."SUMMARY_FLAG",
       vv."START_DATE_ACTIVE",
       vv."END_DATE_ACTIVE",
       vv."PARENT_FLEX_VALUE_LOW",
       vv."PARENT_FLEX_VALUE_HIGH",
       vv."STRUCTURED_HIERARCHY_LEVEL",
       vv."HIERARCHY_LEVEL",
       vv."COMPILED_VALUE_ATTRIBUTES",
       vv."VALUE_CATEGORY",
       vv."ATTRIBUTE1",
       vv."ATTRIBUTE2",
       vv."ATTRIBUTE3",
       vv."ATTRIBUTE4",
       vv."ATTRIBUTE5",
       vv."ATTRIBUTE6",
       vv."ATTRIBUTE7",
       vv."ATTRIBUTE8",
       vv."ATTRIBUTE9",
       vv."ATTRIBUTE10",
       vv."ATTRIBUTE11",
       vv."ATTRIBUTE12",
       vv."ATTRIBUTE13",
       vv."ATTRIBUTE14",
       vv."ATTRIBUTE15",
       vv."ATTRIBUTE16",
       vv."ATTRIBUTE17",
       vv."ATTRIBUTE18",
       vv."ATTRIBUTE19",
       vv."ATTRIBUTE20",
       vv."ATTRIBUTE21",
       vv."ATTRIBUTE22",
       vv."ATTRIBUTE23",
       vv."ATTRIBUTE24",
       vv."ATTRIBUTE25",
       vv."ATTRIBUTE26",
       vv."ATTRIBUTE27",
       vv."ATTRIBUTE28",
       vv."ATTRIBUTE29",
       vv."ATTRIBUTE30",
       vv."ATTRIBUTE31",
       vv."ATTRIBUTE32",
       vv."ATTRIBUTE33",
       vv."ATTRIBUTE34",
       vv."ATTRIBUTE35",
       vv."ATTRIBUTE36",
       vv."ATTRIBUTE37",
       vv."ATTRIBUTE38",
       vv."ATTRIBUTE39",
       vv."ATTRIBUTE40",
       vv."ATTRIBUTE41",
       vv."ATTRIBUTE42",
       vv."ATTRIBUTE43",
       vv."ATTRIBUTE44",
       vv."ATTRIBUTE45",
       vv."ATTRIBUTE46",
       vv."ATTRIBUTE47",
       vv."ATTRIBUTE48",
       vv."ATTRIBUTE49",
       vv."ATTRIBUTE50",
       vv."FLEX_VALUE_MEANING",
       vv."DESCRIPTION",
       vv."ATTRIBUTE_SORT_ORDER"
  FROM fnd_flex_value_sets vs,
       fnd_flex_values_vl  vv
 WHERE vs.flex_value_set_id = vv.flex_value_set_id
   AND vs.flex_value_set_name = 'RFP_IME';
