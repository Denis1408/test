//display(rootDF)

// NCATS_ACCIDENT
val NCATS_ACCIDENT = TableSpec(
  explodes = Seq.empty,
  cols = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACCIDENT_NUM", "ACCIDENT_NUM"),
		ColSpec("AGENCY_ACCIDENT_NUM", "AGENCY_ACCIDENT_NUM"),
		ColSpec("EVENT_NUM", "EVENT_NUM"),
		ColSpec("NCATS_CRASH_ID", "NCATS_CRASH_ID"),
		ColSpec("ACC_ORI", "ACC_ORI"),
		ColSpec("E_ACCIDENT", "E_ACCIDENT"),
		ColSpec("ACCIDENT_CLASS", "ACCIDENT_CLASS"),
		ColSpec("EMERGENCY_USE", "EMERGENCY_USE"),
		ColSpec("REPT_MADE_AT_SCENE", "REPT_MADE_AT_SCENE"),
		ColSpec("ACTIVITY_CODE", "ACTIVITY_CODE"),
		ColSpec("REPT_HIT_RUN", "REPT_HIT_RUN"),
		ColSpec("EXTRACTION", "EXTRACTION"),
		ColSpec("CRASH_SEVERITY", "CRASH_SEVERITY"),
		ColSpec("CRASH_DATE", "CRASH_DATE"),
		ColSpec("CRASH_TIME", "CRASH_TIME"),
		ColSpec("CRASH_DAY", "CRASH_DAY"),
		ColSpec("SECTOR", "SECTOR"),
		ColSpec("BEAT", "BEAT"),
		ColSpec("BEAT_SECTOR", "BEAT_SECTOR"),
		ColSpec("X_COORD", "X_COORD"),
		ColSpec("Y_COORD", "Y_COORD"),
		ColSpec("NODE_NUMBER", "NODE_NUMBER"),
		ColSpec("SEGMENT_NUMBER", "SEGMENT_NUMBER"),
		ColSpec("GEO_CODED", "GEO_CODED"),
		ColSpec("CRASH_CNTY", "CRASH_CNTY"),
		ColSpec("CITY_TOWN_IND", "CITY_TOWN_IND"),
		ColSpec("CRASH_CITY", "CRASH_CITY"),
		ColSpec("MILE_MARKER", "MILE_MARKER"),
		ColSpec("SYS_MILE_MARKER", "SYS_MILE_MARKER"),
		ColSpec("NUM_VEHICLES", "NUM_VEHICLES"),
		ColSpec("NUM_NONMOTORIST", "NUM_NONMOTORIST"),
		ColSpec("NUM_OCCUPANTS", "NUM_OCCUPANTS"),
		ColSpec("NUM_FATALITIES", "NUM_FATALITIES"),
		ColSpec("NUM_INJURED", "NUM_INJURED"),
		ColSpec("NUM_RESTRAINED", "NUM_RESTRAINED"),
		ColSpec("OCCUR_ST_NAME", "OCCUR_ST_NAME"),
		ColSpec("OCCUR_ST_CODE", "OCCUR_ST_CODE"),
		ColSpec("OCCUR_ST_NAME_RPT", "OCCUR_ST_NAME_RPT"),
		ColSpec("INTRSEC_ST_NAME", "INTRSEC_ST_NAME"),
		ColSpec("INTRSEC_ST_CODE", "INTRSEC_ST_CODE"),
		ColSpec("INTRSEC_ST_NAME_RPT", "INTRSEC_ST_NAME_RPT"),
		ColSpec("DISTANCE", "DISTANCE"),
		ColSpec("DISTANCE_TYPE", "DISTANCE_TYPE"),
		ColSpec("APPROXIMATE", "APPROXIMATE"),
		ColSpec("PARKING_LOT", "PARKING_LOT"),
		ColSpec("ACTIVE_SCHOOL_ZONE", "ACTIVE_SCHOOL_ZONE"),
		ColSpec("URBAN_RURAL", "URBAN_RURAL"),
		ColSpec("CITED", "CITED"),
		ColSpec("NDOT_STATUS", "NDOT_STATUS"),
		ColSpec("RD_SURF_OTHER", "RD_SURF_OTHER"),
		ColSpec("INTERSECTION", "INTERSECTION"),
		ColSpec("IS_INTERSECTION", "IS_INTERSECTION"),
		ColSpec("PADDLE_MARKERS", "PADDLE_MARKERS"),
		ColSpec("RDWAY_CHR", "RDWAY_CHR"),
		ColSpec("RDWAY_COND_OTH", "RDWAY_COND_OTH"),
		ColSpec("LOC_ON_OFF_RDWY", "LOC_ON_OFF_RDWY"),
		ColSpec("DIR_FROM_STREET", "DIR_FROM_STREET"),
		ColSpec("TOTAL_ALL_LANES", "TOTAL_ALL_LANES"),
		ColSpec("MAIN_ROAD_LANES", "MAIN_ROAD_LANES"),
		ColSpec("CROSS_RD_LANES", "CROSS_RD_LANES"),
		ColSpec("TRVL_PORT_WIDTH", "TRVL_PORT_WIDTH"),
		ColSpec("TURN_LANE_WIDTH", "TURN_LANE_WIDTH"),
		ColSpec("MED_LANE_WIDTH", "MED_LANE_WIDTH"),
		ColSpec("PAVE_SHLDR_IN", "PAVE_SHLDR_IN"),
		ColSpec("PAVE_SHLDR_OUT", "PAVE_SHLDR_OUT"),
		ColSpec("TOTAL_RD_WIDTH", "TOTAL_RD_WIDTH"),
		ColSpec("ACCESS_CTRL", "ACCESS_CTRL"),
		ColSpec("GRADE", "GRADE"),
		ColSpec("GRADE_RELT_TO", "GRADE_RELT_TO"),
		ColSpec("GRADE_PRECENT", "GRADE_PRECENT"),
		ColSpec("PAVE_MARK_OTH", "PAVE_MARK_OTH"),
		ColSpec("TRAFWY_DESCR", "TRAFWY_DESCR"),
		ColSpec("WEATHER_OTHER", "WEATHER_OTHER"),
		ColSpec("LIGHTING_COND", "LIGHTING_COND"),
		ColSpec("LIGHTING_OTHER", "LIGHTING_OTHER"),
		ColSpec("COLLISION_TYPE", "COLLISION_TYPE"),
		ColSpec("LOC_HARM_EVT", "LOC_HARM_EVT"),
		ColSpec("LOC_HARM_EVT_OTH", "LOC_HARM_EVT_OTH"),
		ColSpec("TURN_LANE_NUM", "TURN_LANE_NUM"),
		ColSpec("TRAVEL_LANE_NUM", "TRAVEL_LANE_NUM"),
		ColSpec("FST_HARM_EVENT", "FST_HARM_EVENT"),
		ColSpec("ENVIR_FACTOR_OTH", "ENVIR_FACTOR_OTH"),
		ColSpec("HIGHWAY_FACTOR_OTH", "HIGHWAY_FACTOR_OTH"),
		ColSpec("PRIVATE_PROPERTY", "PRIVATE_PROPERTY"),
		ColSpec("NARRATIVE", "NARRATIVE"),
		ColSpec("INVEST_COMPLETE", "INVEST_COMPLETE"),
		ColSpec("PHOTO_TAKEN", "PHOTO_TAKEN"),
		ColSpec("SCENE_DIAGRAM", "SCENE_DIAGRAM"),
		ColSpec("STATEMENT", "STATEMENT"),
		ColSpec("NUM_STATEMENTS", "NUM_STATEMENTS"),
		ColSpec("DATE_NOTIFIED", "DATE_NOTIFIED"),
		ColSpec("TIME_NOTIFIED", "TIME_NOTIFIED"),
		ColSpec("ARRIVAL_DATE", "ARRIVAL_DATE"),
		ColSpec("ARRIVAL_TIME", "ARRIVAL_TIME"),
		ColSpec("ELAPSE_TIME", "ELAPSE_TIME"),
		ColSpec("INVEST_DATE", "INVEST_DATE"),
		ColSpec("REVIEW_DATE", "REVIEW_DATE"),
		ColSpec("NDOT_SMS_LEGACY", "NDOT_SMS_LEGACY"),
  )
)

// ACC_NON_MOTORIST 
val ACC_NON_MOTORIST = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("ACC_NON_MOTORIST_Id", "^row0_rownum", IntegerType),
		ColSpec("NON_MOTORIST_NUM", "NON_MOTORIST_NUM"),
		ColSpec("AT_FAULT", "AT_FAULT"),
		ColSpec("NON_MOTOR_TYPE_CODE", "NON_MOTOR_TYPE_CODE"),
		ColSpec("NON_CONTACT_PERSON", "NON_CONTACT_PERSON"),
		ColSpec("TRAVELING_ON", "TRAVELING_ON"),
		ColSpec("TRVL_DIR", "TRVL_DIR"),
		ColSpec("COND_OTH", "COND_OTH"),
		ColSpec("ACTION", "ACTION"),
		ColSpec("ACTION_OTHER", "ACTION_OTHER"),
		ColSpec("FACTORS_OTHER", "FACTORS_OTHER"),
		ColSpec("LOC_PRI_IMPT", "LOC_PRI_IMPT"),
		ColSpec("LOC_PRI_IMPT_OTH", "LOC_PRI_IMPT_OTH"),
		ColSpec("FST_CONTACT", "FST_CONTACT"),
		ColSpec("SAFE_EQUIP_OTH", "SAFE_EQUIP_OTH"),
		ColSpec("BIKEWAY", "BIKEWAY"),
	)
)

// ACC_VEHICLE 
val ACC_VEHICLE = TableSpec(
  explodes = Seq("ACC_VEHICLE"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("ACC_VEHICLE_Id", "VEH_UNIT_NUM"),
		ColSpec("VEH_UNIT_NUM", "VEH_UNIT_NUM"),
		ColSpec("AT_FAULT", "AT_FAULT"),
		ColSpec("NUM_OCCUPANTS", "NUM_OCCUPANTS"),
		ColSpec("DRICER_DISTRACTED", "DRICER_DISTRACTED"),
		ColSpec("NON_CONTACT_VEHICLE", "NON_CONTACT_VEHICLE"),
		ColSpec("TOWED", "TOWED"),
		ColSpec("REMOVED_TO", "REMOVED_TO"),
		ColSpec("REMOVED_BY", "REMOVED_BY"),
		ColSpec("CNTRL_DEV_OTH", "CNTRL_DEV_OTH"),
		ColSpec("TRAVEL_DIR", "TRAVEL_DIR"),
		ColSpec("TRAVELING_ON", "TRAVELING_ON"),
		ColSpec("TRVL_LAN_NUM", "TRVL_LAN_NUM"),
		ColSpec("NUM_TRANSPORTED", "NUM_TRANSPORTED"),
		ColSpec("FST_CONTACT", "FST_CONTACT"),
		ColSpec("DAMAGE_AREA_OTH", "DAMAGE_AREA_OTH"),
		ColSpec("EXTENT_DAMAGE", "EXTENT_DAMAGE"),
		ColSpec("EST_SPEED_HIGH", "EST_SPEED_HIGH"),
		ColSpec("EST_SPEED_LOW", "EST_SPEED_LOW"),
		ColSpec("POSTED_SPEED", "POSTED_SPEED"),
		ColSpec("MOST_HARM_EVENT", "MOST_HARM_EVENT"),
		ColSpec("DIST_AFTER_IMPT", "DIST_AFTER_IMPT"),
		ColSpec("DISTANCE_TYPE", "DISTANCE_TYPE"),
		ColSpec("VEH_ACTION", "VEH_ACTION"),
		ColSpec("VEH_CONFIG", "VEH_CONFIG"),
		ColSpec("VEHICLE_ROLE", "VEHICLE_ROLE"),
		ColSpec("VEH_FACTOR_OTH", "VEH_FACTOR_OTH"),
		ColSpec("DISTANCE_INCHES", "DISTANCE_INCHES"),
	)
)

// ADDRESS1
val ADDRESS1 = TableSpec(
  explodes = Seq("ACC_VEHICLE","OCCUPANT","PERSON_INFO","ADDRESS"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PERSON_INFO_Id", "^row1.PARTY_NUM"),
		ColSpec("WITNESS_Id", "WITNESS_Id"),
		ColSpec("WITNESS_INFO_Id", "WITNESS_INFO_Id"),
	),
  cols = Seq(
		ColSpec("ADDR_TYPE", "ADDR_TYPE"),
		ColSpec("ST_ADDR", "ST_ADDR"),
		ColSpec("CITY", "CITY"),
		ColSpec("COUNTY", "COUNTY"),
		ColSpec("STATE", "STATE"),
		ColSpec("COUNTRY", "COUNTRY"),
		ColSpec("ZIP", "ZIP"),
	),
)

// ADDRESS2
val ADDRESS2 = TableSpec(
  explodes = Seq("WITNESS","WITNESS_INFO","ADDRESS"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PERSON_INFO_Id", "PERSON_INFO_Id"),
		ColSpec("WITNESS_Id", "^row0_rownum", IntegerType, rowNumber = true),
		ColSpec("WITNESS_INFO_Id", "^row0_rownum", IntegerType, rowNumber = true),
	),
  cols = Seq(
		ColSpec("ADDR_TYPE", "ADDR_TYPE"),
		ColSpec("ST_ADDR", "ST_ADDR"),
		ColSpec("CITY", "CITY"),
		ColSpec("COUNTY", "COUNTY"),
		ColSpec("STATE", "STATE"),
		ColSpec("COUNTRY", "COUNTRY"),
		ColSpec("ZIP", "ZIP"),
	),
)

// ADDRESS3
val ADDRESS3 = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","PERSON_INFO","ADDRESS"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PERSON_INFO_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("WITNESS_Id", "WITNESS_Id"),
		ColSpec("WITNESS_INFO_Id", "WITNESS_INFO_Id"),
	),
  cols = Seq(
		ColSpec("ADDR_TYPE", "ADDR_TYPE"),
		ColSpec("ST_ADDR", "ST_ADDR"),
		ColSpec("CITY", "CITY"),
		ColSpec("COUNTY", "COUNTY"),
		ColSpec("STATE", "STATE"),
		ColSpec("COUNTRY", "COUNTRY"),
		ColSpec("ZIP", "ZIP"),
	),
)

// ALCOHOL_DRUG_TESTING1
val ALCOHOL_DRUG_TESTING1 = TableSpec(
  explodes = Seq("ACC_VEHICLE","OCCUPANT","PERSON_INFO","ALCOHOL_DRUG_TESTING"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PERSON_INFO_Id", "^row1.PARTY_NUM"),
		ColSpec("WITNESS_Id", "WITNESS_Id"),
		ColSpec("WITNESS_INFO_Id", "WITNESS_INFO_Id"),
	),
  cols = Seq(
		ColSpec("ALCO_DRUG_FLAG", "ALCO_DRUG_FLAG"),
		ColSpec("BAC_TEST_TYPE", "BAC_TEST_TYPE"),
		ColSpec("TESTING_VALUE", "TESTING_VALUE"),
	),
)

// ALCOHOL_DRUG_TESTING2
val ALCOHOL_DRUG_TESTING2 = TableSpec(
  explodes = Seq("WITNESS","WITNESS_INFO","ALCOHOL_DRUG_TESTING"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PERSON_INFO_Id", "PERSON_INFO_Id"),
		ColSpec("WITNESS_Id", "^row0_rownum", IntegerType, rowNumber = true),
		ColSpec("WITNESS_INFO_Id", "^row0_rownum", IntegerType, rowNumber = true),
	),
  cols = Seq(
		ColSpec("ALCO_DRUG_FLAG", "ALCO_DRUG_FLAG"),
		ColSpec("BAC_TEST_TYPE", "BAC_TEST_TYPE"),
		ColSpec("TESTING_VALUE", "TESTING_VALUE"),
	),
)

// CARGO_BODY_TYPE 
val CARGO_BODY_TYPE = TableSpec(
  explodes = Seq("ACC_VEHICLE","COMM_VEH_INFO"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("COMM_VEH_INFO_Id", "^row1_rownum", IntegerType, rowNumber = true),
	),
  cols = Seq(
		ColSpec("CARGO_BODY_TYPE", "CARGO_BODY_TYPE"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "CARGO_BODY_TYPE_Id",
		partitionBy = Seq("Crash_Id")
	)
  )
)

// CARRIER_ADDRESS 
val CARRIER_ADDRESS = TableSpec(
  explodes = Seq("ACC_VEHICLE","COMM_VEH_INFO","CARRIER_ADDRESS"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("COMM_VEH_INFO_Id", "^row1_rownum", IntegerType, rowNumber = true),
	),
  cols = Seq(
		ColSpec("ADDR_TYPE", "ADDR_TYPE"),
		ColSpec("ST_ADDR", "ST_ADDR"),
		ColSpec("CITY", "CITY"),
		ColSpec("COUNTY", "COUNTY"),
		ColSpec("STATE", "STATE"),
		ColSpec("COUNTRY", "COUNTRY"),
		ColSpec("ZIP", "ZIP"),
	)
)

// CITATION_NUM1
val CITATION_NUM1 = TableSpec(
  explodes = Seq("ACC_VEHICLE","OCCUPANT","CITATION_NUM"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "ACC_NON_MOTORIST_Id"),
		ColSpec("OCCUPANT_Id", "^row1.PARTY_NUM"),
	),
  cols = Seq(
		ColSpec("CITATION_NUM", "^row2"),
	),
)

// CITATION_NUM2
val CITATION_NUM2 = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","CITATION_NUM"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "^row0_rownum", IntegerType),
		ColSpec("OCCUPANT_Id", "OCCUPANT_Id"),
	),
  cols = Seq(
		ColSpec("CITATION_NUM", "^row1"),
	),
)

// COMM_SOURCE 
val COMM_SOURCE = TableSpec(
  explodes = Seq("ACC_VEHICLE","COMM_VEH_INFO"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("COMM_VEH_INFO_Id", "^row1_rownum", IntegerType, rowNumber = true),
	),
  cols = Seq(
		ColSpec("COMM_SOURCE", "COMM_SOURCE"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "COMM_SOURCE_Id",
		//partitionBy = Seq("Crash_Id","ACC_VEHICLE_Id","COMM_VEH_INFO_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
)

// COMM_VEH_INFO 
val COMM_VEH_INFO = TableSpec(
  explodes = Seq("ACC_VEHICLE","COMM_VEH_INFO"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("COMM_VEH_INFO_Id", "^row1_rownum", IntegerType, rowNumber = true),
	),
  cols = Seq(
		ColSpec("SCHOOL_BUS", "SCHOOL_BUS"),
		ColSpec("HAZMAT", "HAZMAT"),
		ColSpec("HAZMAT_RELEASE", "HAZMAT_RELEASE"),
		ColSpec("HAZMAT_PLACARD", "HAZMAT_PLACARD"),
		ColSpec("DIAMOND_NUM", "DIAMOND_NUM"),
		ColSpec("GVWR", "GVWR"),
		ColSpec("CARRIER_NAME", "CARRIER_NAME"),
		ColSpec("NAS_SAFETY_REPT", "NAS_SAFETY_REPT"),
		ColSpec("ICCMC_CARR_NUM", "ICCMC_CARR_NUM"),
		ColSpec("ST_CARR_NUM", "ST_CARR_NUM"),
		ColSpec("DOT_CARR_NUM", "DOT_CARR_NUM"),
		ColSpec("CAN_CARR_NUM", "CAN_CARR_NUM"),
		ColSpec("MEXICO_CARR_NUM", "MEXICO_CARR_NUM"),
		ColSpec("NO_CARRIER_NUM", "NO_CARRIER_NUM"),
		ColSpec("HAZMAT_CLASS", "HAZMAT_CLASS"),
	),
)

// CONDITION 
val CONDITION = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "^row0_rownum", IntegerType),
	),
  cols = Seq(
		ColSpec("CONDITION", "CONDITION"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "CONDITION_Id",
		//partitionBy = Seq("Crash_Id","ACC_NON_MOTORIST_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
)

// CONT_VEH_CODE 
val CONT_VEH_CODE = TableSpec(
  explodes = Seq("ACC_VEHICLE","CONT_VEH_CODE"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
	),
  cols = Seq(
		ColSpec("CONT_VEH_CODE", "^row1"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "CONT_VEH_CODE_Id",
		//partitionBy = Seq("Crash_Id","ACC_VEHICLE_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
)

// DAMAGE_AREA1
val DAMAGE_AREA1 = TableSpec(
  explodes = Seq("ACC_VEHICLE","DAMAGE_AREA"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("NON_MOTOR_VEH_Id", "NON_MOTOR_VEH_Id"),
	),
  cols = Seq(
		ColSpec("DAMAGE_AREA", "^row1"),
	),
)

// DAMAGE_AREA2 
val DAMAGE_AREA2 = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","NON_MOTOR_VEH","DAMAGE_AREA"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "ACC_VEHICLE_Id"),
		ColSpec("NON_MOTOR_VEH_Id", "^row1_rownum", IntegerType, rowNumber = true),
	),
  cols = Seq(
		ColSpec("DAMAGE_AREA", "^row2"),
	),
)

// DRIVER_FACTOR 
val DRIVER_FACTOR = TableSpec(
  explodes = Seq("ACC_VEHICLE","DRIVER_FACTOR"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
	),
  cols = Seq(
		ColSpec("DRIVER_FACTOR", "^row1"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "DRIVER_FACTOR_Id",
		//partitionBy = Seq("Crash_Id","ACC_VEHICLE_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
)

// DRIVER_LIC1
val DRIVER_LIC1 = TableSpec(
  explodes = Seq("ACC_VEHICLE","DRIVER_LIC"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "ACC_NON_MOTORIST_Id"),
		//ColSpec("DRIVER_LIC_Id", "^row0.VEH_UNIT_NUM"),
	),
  cols = Seq(
		ColSpec("OLN", "OLN"),
		ColSpec("TYPE", "TYPE"),
		ColSpec("STATE", "STATE"),
		ColSpec("STATUS", "STATUS"),
		ColSpec("DL_COMMERCIAL", "DL_COMMERCIAL"),
		ColSpec("DL_CLASS", "DL_CLASS"),
		ColSpec("EXPIRE_DATE", "EXPIRE_DATE"),
		ColSpec("COMP_ENDORSE", "COMP_ENDORSE"),
		ColSpec("COMP_RESTRICT", "COMP_RESTRICT"),
		ColSpec("DLCLASS", "DLCLASS"),
	),
)

// DRIVER_LIC2
val DRIVER_LIC2 = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","DRIVER_LIC"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "ACC_VEHICLE_Id"),
		ColSpec("ACC_NON_MOTORIST_Id", "^row0_rownum", IntegerType),
		//ColSpec("DRIVER_LIC_Id", "^row0.NON_MOTORIST_NUM"),
	),
  cols = Seq(
		ColSpec("OLN", "OLN"),
		ColSpec("TYPE", "TYPE"),
		ColSpec("STATE", "STATE"),
		ColSpec("STATUS", "STATUS"),
		ColSpec("DL_COMMERCIAL", "DL_COMMERCIAL"),
		ColSpec("DL_CLASS", "DL_CLASS"),
		ColSpec("EXPIRE_DATE", "EXPIRE_DATE"),
		ColSpec("COMP_ENDORSE", "COMP_ENDORSE"),
		ColSpec("COMP_RESTRICT", "COMP_RESTRICT"),
		ColSpec("DLCLASS", "DLCLASS"),
	),
)

// ENDORSE_CODE 
val ENDORSE_CODE = TableSpec(
  explodes = Seq("ACC_VEHICLE","DRIVER_LIC","ENDORSE_CODE"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		//ColSpec("DRIVER_LIC_Id", "^row1_rownum", IntegerType, rowNumber = true),
		//ColSpec("DRIVER_LIC_Id", "^row0.VEH_UNIT_NUM"),
	),
  cols = Seq(
		ColSpec("ENDORSE_CODE", "row2"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "ENDORSE_CODE_Id",
		//partitionBy = Seq("Crash_Id","DRIVER_LIC_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
)

// ENVIR_FACTOR 
val ENVIR_FACTOR = TableSpec(
 explodes = Seq("ENVIR_FACTOR"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("ENVIR_FACTOR", "row0"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "ENVIR_FACTOR_Id",
		partitionBy = Seq("Crash_Id")
	)
  )
)

// FACTORS 
val FACTORS = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		//ColSpec("ACC_NON_MOTORIST_Id", "NON_MOTORIST_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "^row0_rownum", IntegerType),
	),
  cols = Seq(
		ColSpec("FACTORS", "FACTORS"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "FACTORS_Id",
		//partitionBy = Seq("Crash_Id","ACC_NON_MOTORIST_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
)

// GPS 
val GPS = TableSpec(
  explodes = Seq("GPS"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("GPS_LATIT", "GPS_LATIT"),
		ColSpec("GPS_LONG", "GPS_LONG"),
		ColSpec("GPS_ALTIT", "GPS_ALTIT"),
	),
)

// INJURED_AREA 
val INJURED_AREA1 = TableSpec(
  explodes = Seq("ACC_VEHICLE","OCCUPANT","INJURY","INJURED_AREA"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		//ColSpec("INJURY_Id", "^row1_rownum", IntegerType, rowNumber = true),
		//ColSpec("INJURY_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "ACC_NON_MOTORIST_Id"),
		ColSpec("OCCUPANT_Id", "^row1.PARTY_NUM"),
	),
  cols = Seq(
		ColSpec("INJURED_AREA", "row3"),
	),
  /*
  autoInc = Seq(
	AutoIncConf(
		name = "INJURED_AREA_Id",
		//partitionBy = Seq("Crash_Id","INJURY_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
  */
   autoInc = Seq(
	AutoIncConf(
		name = "INJURY_Id",
		partitionBy = Seq("Crash_Id"),
		orderBy = Seq("ACC_VEHICLE_Id","ACC_NON_MOTORIST_Id","OCCUPANT_Id")
	)
  )
)

// INJURED_AREA2
val INJURED_AREA2 = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","INJURY","INJURED_AREA"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		//ColSpec("INJURY_Id", "^row1_rownum", IntegerType, rowNumber = true),
		//ColSpec("INJURY_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "ACC_VEHICLE_Id"),
		ColSpec("ACC_NON_MOTORIST_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("OCCUPANT_Id", "OCCUPANT_Id"),
	),
  cols = Seq(
		ColSpec("INJURED_AREA", "row2"),
	),
  /*
  autoInc = Seq(
	AutoIncConf(
		name = "INJURED_AREA_Id",
		//partitionBy = Seq("Crash_Id","INJURY_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
  */
   autoInc = Seq(
	AutoIncConf(
		name = "INJURY_Id",
		partitionBy = Seq("Crash_Id"),
		orderBy = Seq("ACC_VEHICLE_Id","ACC_NON_MOTORIST_Id","OCCUPANT_Id")
	)
  )
)

// INJURY1 
val INJURY1 = TableSpec(
  explodes = Seq("ACC_VEHICLE","OCCUPANT","INJURY"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		//ColSpec("INJURY_Id", "^row2_rownum", IntegerType, rowNumber = true),
		//ColSpec("INJURY_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "ACC_NON_MOTORIST_Id"),
		ColSpec("OCCUPANT_Id", "^row1.PARTY_NUM"),
	),
  cols = Seq(
		ColSpec("TRANS_TO", "TRANS_TO"),
		ColSpec("TRANS_BY", "TRANS_BY"),
		ColSpec("TAKEN_BY_OTHER", "TAKEN_BY_OTHER"),
		ColSpec("EMS_NAME", "EMS_NAME"),
		ColSpec("EMS_UNIT_NUM", "EMS_UNIT_NUM"),
		ColSpec("INJURED_STATUS", "INJURED_STATUS"),
	),
)

// INJURY2
val INJURY2 = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","INJURY"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "ACC_VEHICLE_Id"),
		//ColSpec("INJURY_Id", "^row1_rownum", IntegerType, rowNumber = true),
		//ColSpec("ACC_NON_MOTORIST_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "^row0_rownum", IntegerType),
		ColSpec("OCCUPANT_Id", "OCCUPANT_Id"),
	),
  cols = Seq(
		ColSpec("TRANS_TO", "TRANS_TO"),
		ColSpec("TRANS_BY", "TRANS_BY"),
		ColSpec("TAKEN_BY_OTHER", "TAKEN_BY_OTHER"),
		ColSpec("EMS_NAME", "EMS_NAME"),
		ColSpec("EMS_UNIT_NUM", "EMS_UNIT_NUM"),
		ColSpec("INJURED_STATUS", "INJURED_STATUS"),
	),
)

// INVESTIGATOR 
val INVESTIGATOR = TableSpec(
  explodes = Seq("INVESTIGATOR"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("OFR_NAM", "OFR_NAM"),
		ColSpec("OFR_BGE_NUM", "OFR_BGE_NUM"),
		ColSpec("SUBSTATION_CODE", "SUBSTATION_CODE"),
		ColSpec("OFR_AGY_CODE", "OFR_AGY_CODE"),
	),
)

// NAME1
val NAME1 = TableSpec(
  explodes = Seq("ACC_VEHICLE","OCCUPANT","PERSON_INFO","NAME"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PERSON_INFO_Id", "^row1.PARTY_NUM"),
		ColSpec("WITNESS_Id", "WITNESS_Id"),
		ColSpec("WITNESS_INFO_Id", "WITNESS_INFO_Id"),
	),
  cols = Seq(
		ColSpec("LAST_NAME", "LAST_NAME"),
		ColSpec("FIRST_NAME", "FIRST_NAME"),
		ColSpec("MIDDLE_NAME", "MIDDLE_NAME"),
		ColSpec("SUFFIX_NAME", "SUFFIX_NAME"),
		ColSpec("LAST_NAME_SDX", "LAST_NAME_SDX"),
		ColSpec("FIRST_NAME_SDX", "FIRST_NAME_SDX"),
	),
)

// NAME2
val NAME2 = TableSpec(
  explodes = Seq("WITNESS","WITNESS_INFO","NAME"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PERSON_INFO_Id", "PERSON_INFO_Id"),
		ColSpec("WITNESS_Id", "^row0_rownum", IntegerType, rowNumber = true),
		ColSpec("WITNESS_INFO_Id", "^row0_rownum", IntegerType, rowNumber = true),
	),
  cols = Seq(
		ColSpec("LAST_NAME", "LAST_NAME"),
		ColSpec("FIRST_NAME", "FIRST_NAME"),
		ColSpec("MIDDLE_NAME", "MIDDLE_NAME"),
		ColSpec("SUFFIX_NAME", "SUFFIX_NAME"),
		ColSpec("LAST_NAME_SDX", "LAST_NAME_SDX"),
		ColSpec("FIRST_NAME_SDX", "FIRST_NAME_SDX"),
	),
)

// NAME3
val NAME3 = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","PERSON_INFO","NAME"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PERSON_INFO_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("WITNESS_Id", "WITNESS_Id"),
		ColSpec("WITNESS_INFO_Id", "WITNESS_INFO_Id"),
	),
  cols = Seq(
		ColSpec("LAST_NAME", "LAST_NAME"),
		ColSpec("FIRST_NAME", "FIRST_NAME"),
		ColSpec("MIDDLE_NAME", "MIDDLE_NAME"),
		ColSpec("SUFFIX_NAME", "SUFFIX_NAME"),
		ColSpec("LAST_NAME_SDX", "LAST_NAME_SDX"),
		ColSpec("FIRST_NAME_SDX", "FIRST_NAME_SDX"),
	),
)

// NON_MOTOR_VEH 
val NON_MOTOR_VEH = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","NON_MOTOR_VEH"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		//ColSpec("ACC_NON_MOTORIST_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "^row0_rownum", IntegerType),
		ColSpec("NON_MOTOR_VEH_Id", "^row1_rownum", IntegerType, rowNumber = true),
	),
  cols = Seq(
		ColSpec("NMV_ID_NUMBER", "NMV_ID_NUMBER"),
		ColSpec("NMV_MAKE", "NMV_MAKE"),
		ColSpec("NMV_MODEL", "NMV_MODEL"),
		ColSpec("NMV_COLOR", "NMV_COLOR"),
		ColSpec("EXTENT_DAMAGE", "EXTENT_DAMAGE"),
		ColSpec("VEH_ACTION", "VEH_ACTION"),
		ColSpec("VEH_ACTION_OTH", "VEH_ACTION_OTH"),
		ColSpec("DMG_AREA_OTH", "DMG_AREA_OTH"),
		ColSpec("VEH_TYPE", "VEH_TYPE"),
		ColSpec("FST_CONTACT", "FST_CONTACT"),
		ColSpec("REMOVED_TO", "REMOVED_TO"),
		ColSpec("REMOVED_BY", "REMOVED_BY"),
		ColSpec("EST_SPEED_LOW", "EST_SPEED_LOW"),
		ColSpec("EST_SPEED_HIGH", "EST_SPEED_HIGH"),
		ColSpec("POSTED_SPEED", "POSTED_SPEED"),
		ColSpec("MOST_HARM_EVENT", "MOST_HARM_EVENT"),
	),
  /*
  autoInc = Seq(
	AutoIncConf(
		name = "NON_MOTOR_VEH_Id",
		//partitionBy = Seq("Crash_Id","ACC_NON_MOTORIST_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
  */
)

// OCCUPANT 
val OCCUPANT = TableSpec(
  explodes = Seq("ACC_VEHICLE","OCCUPANT"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("OCCUPANT_Id", "PARTY_NUM"),
	),
  cols = Seq(
		ColSpec("PARTY_NUM", "PARTY_NUM"),
		ColSpec("CITATION_NUM", "CITATION_NUM"),
		ColSpec("AIRBAGS", "AIRBAGS"),
		ColSpec("AIRBAG_SWITCH", "AIRBAG_SWITCH"),
		ColSpec("OCCUPANT_REST", "OCCUPANT_REST"),
		ColSpec("EJECTED", "EJECTED"),
		ColSpec("SEAT_POSITION", "SEAT_POSITION"),
		ColSpec("TRAPPED", "TRAPPED"),
		ColSpec("VISION_OBSCUR_BY", "VISION_OBSCUR_BY"),
		ColSpec("CORONER_AGENCY", "CORONER_AGENCY"),
		ColSpec("CORONER_NAME", "CORONER_NAME"),
		ColSpec("MORTUARY", "MORTUARY"),
		ColSpec("OCCUPANT_HELM", "OCCUPANT_HELM"),
	),
)

// OWNER_INFO1
val OWNER_INFO1 = TableSpec(
  explodes = Seq("ACC_VEHICLE","VEH_INFO","OWNER_INFO"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PROPERTY_Id", "PROPERTY_Id"),
		ColSpec("VEH_INFO_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "ACC_NON_MOTORIST_Id"),
		ColSpec("NON_MOTOR_VEH_Id", "NON_MOTOR_VEH_Id"),
	),
  cols = Seq(
		ColSpec("LAST_NAME", "OWNER_NAME.LAST_NAME"),
		ColSpec("FIRST_NAME", "OWNER_NAME.FIRST_NAME"),
		ColSpec("MIDDLE_NAME", "OWNER_NAME.MIDDLE_NAME"),
		ColSpec("SUFFIX_NAME", "OWNER_NAME.SUFFIX_NAME"),
		ColSpec("ADDR_TYPE", "OWNER_ADDR.ADDR_TYPE"),
		ColSpec("ST_ADDR", "OWNER_ADDR.ST_ADDR"),
		ColSpec("CITY", "OWNER_ADDR.CITY"),
		ColSpec("COUNTY", "OWNER_ADDR.COUNTY"),
		ColSpec("STATE", "OWNER_ADDR.STATE"),
		ColSpec("COUNTRY", "OWNER_ADDR.COUNTRY"),
		ColSpec("ZIP", "OWNER_ADDR.ZIP"),
	),
)

// OWNER_INFO2
val OWNER_INFO2 = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","NON_MOTOR_VEH","OWNER_INFO"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PROPERTY_Id", "PROPERTY_Id"),
		ColSpec("VEH_INFO_Id", "VEH_INFO_Id"),
		//ColSpec("ACC_NON_MOTORIST_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "^row0_rownum", IntegerType),
		//ColSpec("NON_MOTOR_VEH_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("NON_MOTOR_VEH_Id", "^row1_rownum", IntegerType, rowNumber = true),
	),
  cols = Seq(
		ColSpec("LAST_NAME", "OWNER_NAME.LAST_NAME"),
		ColSpec("FIRST_NAME", "OWNER_NAME.FIRST_NAME"),
		ColSpec("MIDDLE_NAME", "OWNER_NAME.MIDDLE_NAME"),
		ColSpec("SUFFIX_NAME", "OWNER_NAME.SUFFIX_NAME"),
		ColSpec("ADDR_TYPE", "OWNER_ADDR.ADDR_TYPE"),
		ColSpec("ST_ADDR", "OWNER_ADDR.ST_ADDR"),
		ColSpec("CITY", "OWNER_ADDR.CITY"),
		ColSpec("COUNTY", "OWNER_ADDR.COUNTY"),
		ColSpec("STATE", "OWNER_ADDR.STATE"),
		ColSpec("COUNTRY", "OWNER_ADDR.COUNTRY"),
		ColSpec("ZIP", "OWNER_ADDR.ZIP"),
	),
)

// OWNER_INFO3
val OWNER_INFO3 = TableSpec(
  explodes = Seq("PROPERTY","OWNER_INFO"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PROPERTY_Id", "^row1_rownum", IntegerType, rowNumber = true),
		ColSpec("VEH_INFO_Id", "VEH_INFO_Id"),
		ColSpec("ACC_NON_MOTORIST_Id", "ACC_NON_MOTORIST_Id"),
		ColSpec("NON_MOTOR_VEH_Id", "NON_MOTOR_VEH_Id"),
	),
  cols = Seq(
		ColSpec("LAST_NAME", "OWNER_NAME.LAST_NAME"),
		ColSpec("FIRST_NAME", "OWNER_NAME.FIRST_NAME"),
		ColSpec("MIDDLE_NAME", "OWNER_NAME.MIDDLE_NAME"),
		ColSpec("SUFFIX_NAME", "OWNER_NAME.SUFFIX_NAME"),
		ColSpec("ADDR_TYPE", "OWNER_ADDR.ADDR_TYPE"),
		ColSpec("ST_ADDR", "OWNER_ADDR.ST_ADDR"),
		ColSpec("CITY", "OWNER_ADDR.CITY"),
		ColSpec("COUNTY", "OWNER_ADDR.COUNTY"),
		ColSpec("STATE", "OWNER_ADDR.STATE"),
		ColSpec("COUNTRY", "OWNER_ADDR.COUNTRY"),
		ColSpec("ZIP", "OWNER_ADDR.ZIP"),
	),
)

// PAVE_TRAFF_MARK 
val PAVE_TRAFF_MARK = TableSpec(
  explodes = Seq("PAVE_TRAFF_MARK"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("PAVE_MARK", "PAVE_MARK"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "PAVE_TRAFF_MARK_Id",
		partitionBy = Seq("Crash_Id")
	)
  )
)

// PERSON_INFO1
val PERSON_INFO1 = TableSpec(
  explodes = Seq("ACC_VEHICLE","OCCUPANT","PERSON_INFO"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		//ColSpec("PERSON_INFO_Id", "^row1.PARTY_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("OCCUPANT_Id", "^row1.PARTY_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "ACC_NON_MOTORIST_Id"),
	),
  cols = Seq(
		ColSpec("TYPE_CODE", "TYPE_CODE"),
		ColSpec("TYPE_OTHER", "TYPE_OTHER"),
		ColSpec("DOB", "DOB"),
		ColSpec("DOB_UNK", "DOB_UNK"),
		ColSpec("BIRTH_STATE", "BIRTH_STATE"),
		ColSpec("AGE", "AGE"),
		ColSpec("SEX", "SEX"),
		ColSpec("HEIGHT", "HEIGHT"),
		ColSpec("WEIGHT", "WEIGHT"),
		ColSpec("ALCO_DRUG_SUSPC", "ALCO_DRUG_SUSPC"),
		ColSpec("DL_INDICATOR", "DL_INDICATOR"),
	),
)

// PERSON_INFO2
val PERSON_INFO2 = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","PERSON_INFO"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		//ColSpec("PERSON_INFO_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("ACC_VEHICLE_Id", "ACC_VEHICLE_Id"),
		ColSpec("OCCUPANT_Id", "OCCUPANT_Id"),
		//ColSpec("ACC_NON_MOTORIST_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "^row0_rownum", IntegerType),
	),
  cols = Seq(
		ColSpec("TYPE_CODE", "TYPE_CODE"),
		ColSpec("TYPE_OTHER", "TYPE_OTHER"),
		ColSpec("DOB", "DOB"),
		ColSpec("DOB_UNK", "DOB_UNK"),
		ColSpec("BIRTH_STATE", "BIRTH_STATE"),
		ColSpec("AGE", "AGE"),
		ColSpec("SEX", "SEX"),
		ColSpec("HEIGHT", "HEIGHT"),
		ColSpec("WEIGHT", "WEIGHT"),
		ColSpec("ALCO_DRUG_SUSPC", "ALCO_DRUG_SUSPC"),
		ColSpec("DL_INDICATOR", "DL_INDICATOR"),
	),
)

// PHONE1
val PHONE1 = TableSpec(
  explodes = Seq("ACC_VEHICLE","OCCUPANT","PERSON_INFO","PHONE"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PERSON_INFO_Id", "^row1.PARTY_NUM"),
		ColSpec("WITNESS_Id", "WITNESS_Id"),
		ColSpec("WITNESS_INFO_Id", "WITNESS_INFO_Id"),
	),
  cols = Seq(
		ColSpec("PHONE_TYP", "PHONE_TYP"),
		ColSpec("PHONE_NUM", "PHONE_NUM"),
		ColSpec("PHONE_COUNTRY", "PHONE_COUNTRY"),
	),
)

// PHONE2
val PHONE2 = TableSpec(
  explodes = Seq("WITNESS","WITNESS_INFO","PHONE"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PERSON_INFO_Id", "PERSON_INFO_Id"),
		ColSpec("WITNESS_Id", "^row0_rownum", IntegerType, rowNumber = true),
		ColSpec("WITNESS_INFO_Id", "^row0_rownum", IntegerType, rowNumber = true),
	),
  cols = Seq(
		ColSpec("PHONE_TYP", "PHONE_TYP"),
		ColSpec("PHONE_NUM", "PHONE_NUM"),
		ColSpec("PHONE_COUNTRY", "PHONE_COUNTRY"),
	),
)

// PHONE3
val PHONE3 = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","PERSON_INFO","PHONE"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("PERSON_INFO_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("WITNESS_Id", "WITNESS_Id"),
		ColSpec("WITNESS_INFO_Id", "WITNESS_INFO_Id"),
	),
  cols = Seq(
		ColSpec("PHONE_TYP", "PHONE_TYP"),
		ColSpec("PHONE_NUM", "PHONE_NUM"),
		ColSpec("PHONE_COUNTRY", "PHONE_COUNTRY"),
	),
)

// PROPERTY 
val PROPERTY = TableSpec(
  explodes = Seq("PROPERTY"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("OWNER_NOTIFIED", "OWNER_NOTIFIED"),
		ColSpec("DAMAGE_DESC", "DAMAGE_DESC"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "PROPERTY_Id",
		partitionBy = Seq("Crash_Id")
	)
  )
)

// RD_SURF_TYPE 
val RD_SURF_TYPE = TableSpec(
  explodes = Seq("RD_SURF_TYPE"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("RD_SURF_TYPE", "^row0"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "RD_SURF_TYPE_Id",
		partitionBy = Seq("Crash_Id")
	)
  )
)

// RDWAY_COND 
val RDWAY_COND = TableSpec(
  explodes = Seq("RDWAY_COND"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("RDWAY_COND", "^row0"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "RDWAY_COND_Id",
		partitionBy = Seq("Crash_Id")
	)
  )
)

// RESTRICTION 
val RESTRICTION = TableSpec(
  explodes = Seq("ACC_VEHICLE","DRIVER_LIC","RESTRICTION"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		//ColSpec("DRIVER_LIC_Id", "^row0.VEH_UNIT_NUM"),
	),
  cols = Seq(
		ColSpec("RESTRICTION", "row2"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "RESTRICTION_Id",
		partitionBy = Seq("Crash_Id")
	)
  )
)

// REVIEWED_BY 
val REVIEWED_BY = TableSpec(
  explodes = Seq("REVIEWED_BY"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("OFR_NAM", "OFR_NAM"),
		ColSpec("OFR_BGE_NUM", "OFR_BGE_NUM"),
		ColSpec("SUBSTATION_CODE", "SUBSTATION_CODE"),
		ColSpec("OFR_AGY_CODE", "OFR_AGY_CODE"),
	),
)

// SAFE_EQUIP 
val SAFE_EQUIP = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","SAFE_EQUIP"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		//ColSpec("ACC_NON_MOTORIST_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "^row0_rownum", IntegerType),
	),
  cols = Seq(
		ColSpec("SAFE_EQUIP", "^row1"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "SAFE_EQUIP_Id",
		partitionBy = Seq("Crash_Id")
	)
  )
)

// SEQ_OF_EVENT1
val SEQ_OF_EVENT1 = TableSpec(
  explodes = Seq("ACC_VEHICLE","SEQ_OF_EVENT"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "ACC_NON_MOTORIST_Id"),
		ColSpec("NON_MOTOR_VEH_Id", "NON_MOTOR_VEH_Id"),
	),
  cols = Seq(
		ColSpec("SEQ_OF_EVENT", "^row1"),
	),
)

// SEQ_OF_EVENT2
val SEQ_OF_EVENT2 = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","NON_MOTOR_VEH","SEQ_OF_EVENT"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "ACC_VEHICLE_Id"),
		//ColSpec("ACC_NON_MOTORIST_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "^row0_rownum", IntegerType),
		//ColSpec("NON_MOTOR_VEH_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("NON_MOTOR_VEH_Id", "^row1_rownum", IntegerType),
	),
  cols = Seq(
		ColSpec("SEQ_OF_EVENT", "^row2"),
	),
)

// SUPPLEMENTS 
val SUPPLEMENTS = TableSpec(
  explodes = Seq("SUPPLEMENTS"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("SUPPLEMENT", "SUPPLEMENT"),
		ColSpec("SUPPLEMENT_CNT", "SUPPLEMENT_CNT"),
		ColSpec("SUPPLEMENT_PTR", "SUPPLEMENT_PTR"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "SUPPLEMENT_Id",
		partitionBy = Seq("Crash_Id")
	)
  )
)

// TRAF_CNTRL_DEV 
val TRAF_CNTRL_DEV = TableSpec(
  explodes = Seq("ACC_VEHICLE","TRAF_CNTRL_DEV"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
	),
  cols = Seq(
		ColSpec("DEVICE_FUNCTION", "DEVICE_FUNCTION"),
		ColSpec("DEVICE_OBSCURED", "DEVICE_OBSCURED"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "TRAF_CNTRL_DEV_Id",
		//partitionBy = Seq("Crash_Id","ACC_VEHICLE_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
)

// TRAILER 
val TRAILER = TableSpec(
  explodes = Seq("ACC_VEHICLE","TRAILER"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
	),
  cols = Seq(
		ColSpec("TRL_UNIT_NUM", "TRL_UNIT_NUM"),
		ColSpec("TRL_TYPE", "TRL_TYPE"),
		ColSpec("TRR_VIN", "TRR_VIN"),
		ColSpec("TRL_LP_NUM", "TRL_LP_NUM"),
		ColSpec("TRL_REG_ST", "TRL_REG_ST"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "TRAILER_Id",
		//partitionBy = Seq("Crash_Id","ACC_VEHICLE_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
)

// UNDER_OVERRIDE 
val UNDER_OVERRIDE = TableSpec(
  explodes = Seq("ACC_VEHICLE","UNDER_OVERRIDE"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
	),
  cols = Seq(
		ColSpec("UNDER_OVERRIDE", "^row1"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "UNDER_OVERRIDE_Id",
		//partitionBy = Seq("Crash_Id","ACC_VEHICLE_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
)

// VEH_DEFECT_CODE 
val VEH_DEFECT_CODE = TableSpec(
  explodes = Seq("ACC_VEHICLE","VEH_DEFECT_CODE"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
	),
  cols = Seq(
		ColSpec("VEH_DEFECT_CODE", "^row1"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "VEH_DEFECT_CODE_Id",
		//partitionBy = Seq("Crash_Id","ACC_VEHICLE_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
)

// VEH_INFO 
val VEH_INFO = TableSpec(
  explodes = Seq("ACC_VEHICLE","VEH_INFO"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("ACC_VEHICLE_Id", "^row0.VEH_UNIT_NUM"),
		ColSpec("VEH_INFO_Id", "^row0.VEH_UNIT_NUM"),
	),
  cols = Seq(
		ColSpec("COMM_VEH", "COMM_VEH"),
		ColSpec("VIN", "VIN"),
		ColSpec("LIC_PLATE_NUM", "LIC_PLATE_NUM"),
		ColSpec("LIC_PLATE_STATE", "LIC_PLATE_STATE"),
		ColSpec("LIC_EXPIRE_YEAR", "LIC_EXPIRE_YEAR"),
		ColSpec("VEH_YEAR", "VEH_YEAR"),
		ColSpec("VEH_MAKE", "VEH_MAKE"),
		ColSpec("VEH_STYLE", "VEH_STYLE"),
		ColSpec("VEH_MODEL", "VEH_MODEL"),
		ColSpec("VEH_COLOR", "VEH_COLOR"),
		ColSpec("INSURED", "INSURED"),
		ColSpec("INSUR_COM_NAME", "INSUR_COM_NAME"),
		ColSpec("INSUR_END_DATE", "INSUR_END_DATE"),
		ColSpec("INSUR_EFF_DATE", "INSUR_EFF_DATE"),
		ColSpec("INSUR_EXPIRED", "INSUR_EXPIRED"),
		ColSpec("INSUR_POLY_NUM", "INSUR_POLY_NUM"),
		ColSpec("RO_SAME_FLAG", "RO_SAME_FLAG"),
		ColSpec("AV_PRESENCE", "AV_PRESENCE"),
		ColSpec("AV_LEVEL", "AV_LEVEL"),
		ColSpec("AV_ENGAGED", "AV_ENGAGED"),
	),
)

// VEH_NUM_STRIKING 
val VEH_NUM_STRIKING = TableSpec(
  explodes = Seq("ACC_NON_MOTORIST","VEH_NUM_STRIKING"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		//ColSpec("ACC_NON_MOTORIST_Id", "^row0.NON_MOTORIST_NUM"),
		ColSpec("ACC_NON_MOTORIST_Id", "^row0_rownum", IntegerType),
	),
  cols = Seq(
		ColSpec("VEH_NUM_STRIKING", "^row1"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "VEH_NUM_STRIKING_Id",
		//partitionBy = Seq("Crash_Id","ACC_NON_MOTORIST_Id")
		partitionBy = Seq("Crash_Id")
	)
  )
)

// WEATHER_COND 
val WEATHER_COND = TableSpec(
  explodes = Seq("WEATHER_COND"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("WEATHER_COND", "WEATHER_COND"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "WEATHER_COND_Id",
		partitionBy = Seq("Crash_Id")
	)
  )
)

// WITNESS 
val WITNESS = TableSpec(
  explodes = Seq("WITNESS"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
	),
  cols = Seq(
		ColSpec("WITNESS_STATEMENT", "WITNESS_STATEMENT"),
		ColSpec("WITNESS_INFO", "WITNESS_INFO"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "WITNESS_Id",
		partitionBy = Seq("Crash_Id")
	)
  )
)

// WITNESS_INFO 
val WITNESS_INFO = TableSpec(
  explodes = Seq("WITNESS","WITNESS_INFO"),
	carry = Seq(
		ColSpec("Crash_Id", "ACCIDENT_NUM"),
		ColSpec("WITNESS_Id", "^row0_rownum", IntegerType, rowNumber = true),
	),
  cols = Seq(
		ColSpec("TYPE_CODE", "TYPE_CODE"),
		ColSpec("TYPE_OTHER", "TYPE_OTHER"),
		ColSpec("DOB", "DOB"),
		ColSpec("DOB_UNK", "DOB_UNK"),
		ColSpec("BIRTH_STATE", "BIRTH_STATE"),
		ColSpec("AGE", "AGE"),
		ColSpec("SEX", "SEX"),
		ColSpec("HEIGHT", "HEIGHT"),
		ColSpec("WEIGHT", "WEIGHT"),
		ColSpec("ALCO_DRUG_SUSPC", "ALCO_DRUG_SUSPC"),
		ColSpec("DL_INDICATOR", "DL_INDICATOR"),
	),
  autoInc = Seq(
	AutoIncConf(
		name = "WITNESS_INFO_Id",
		partitionBy = Seq("Crash_Id","WITNESS_Id")
	)
  )
)

// Collect all table specs into a map
val tablesSpec: Map[String, TableSpec] = Map(
	"NCATS_ACCIDENT" -> NCATS_ACCIDENT,
	"ACC_NON_MOTORIST" -> ACC_NON_MOTORIST,
	"ACC_VEHICLE" -> ACC_VEHICLE,
	"ADDRESS" -> ADDRESS1,
	"ADDRESS2" -> ADDRESS2,
	"ADDRESS3" -> ADDRESS3,
	"ALCOHOL_DRUG_TESTING" -> ALCOHOL_DRUG_TESTING1,
	"ALCOHOL_DRUG_TESTING2" -> ALCOHOL_DRUG_TESTING2,
	"CARGO_BODY_TYPE" -> CARGO_BODY_TYPE,
	"CARRIER_ADDRESS" -> CARRIER_ADDRESS,
	"CITATION_NUM" -> CITATION_NUM1,
	"CITATION_NUM2" -> CITATION_NUM2,
	"COMM_SOURCE" -> COMM_SOURCE,
	"COMM_VEH_INFO" -> COMM_VEH_INFO,
	"CONDITION" -> CONDITION,
	"CONT_VEH_CODE" -> CONT_VEH_CODE,
	"DAMAGE_AREA" -> DAMAGE_AREA1,
	"DAMAGE_AREA2" -> DAMAGE_AREA2,
	"DRIVER_FACTOR" -> DRIVER_FACTOR,
	"DRIVER_LIC" -> DRIVER_LIC1,
	"DRIVER_LIC2" -> DRIVER_LIC2,
	"ENDORSE_CODE" -> ENDORSE_CODE,
	"ENVIR_FACTOR" -> ENVIR_FACTOR,
	"FACTORS" -> FACTORS,
	"GPS" -> GPS,
	"INJURED_AREA" -> INJURED_AREA1,
	"INJURED_AREA2" -> INJURED_AREA2,
	"INJURY" -> INJURY1,
	"INJURY2" -> INJURY2,
	"INVESTIGATOR" -> INVESTIGATOR,
	"NAME" -> NAME1,
	"NAME2" -> NAME2,
	"NAME3" -> NAME3,
	"NON_MOTOR_VEH" -> NON_MOTOR_VEH,
	"OCCUPANT" -> OCCUPANT,
	"OWNER_INFO" -> OWNER_INFO1,
	"OWNER_INFO2" -> OWNER_INFO2,
	"OWNER_INFO3" -> OWNER_INFO3,
	"PAVE_TRAFF_MARK" -> PAVE_TRAFF_MARK,
	"PERSON_INFO" -> PERSON_INFO1,
	"PERSON_INFO2" -> PERSON_INFO2,
	"PHONE" -> PHONE1,
	"PHONE2" -> PHONE2,
	"PHONE3" -> PHONE3,
	"PROPERTY" -> PROPERTY,
	"RD_SURF_TYPE" -> RD_SURF_TYPE,
	"RDWAY_COND" -> RDWAY_COND,
	"RESTRICTION" -> RESTRICTION,
	"REVIEWED_BY" -> REVIEWED_BY,
	"SAFE_EQUIP" -> SAFE_EQUIP,
	"SEQ_OF_EVENT" -> SEQ_OF_EVENT1,
	"SEQ_OF_EVENT2" -> SEQ_OF_EVENT2,
	"SUPPLEMENTS" -> SUPPLEMENTS,
	"TRAF_CNTRL_DEV" -> TRAF_CNTRL_DEV,
	"TRAILER" -> TRAILER,
	"UNDER_OVERRIDE" -> UNDER_OVERRIDE,
	"VEH_DEFECT_CODE" -> VEH_DEFECT_CODE,
	"VEH_INFO" -> VEH_INFO,
	"VEH_NUM_STRIKING" -> VEH_NUM_STRIKING,
	"WEATHER_COND" -> WEATHER_COND,
	"WITNESS" -> WITNESS,
	"WITNESS_INFO" -> WITNESS_INFO
)



// ================= BUILD + REGISTER =================
val tables_init: Map[String, DataFrame] = tablesSpec.map { case (name, spec) =>
	val df = makeTable(rootDF, spec)
	df.createOrReplaceTempView(s"vw_${name}")
	name -> df
}.toMap



