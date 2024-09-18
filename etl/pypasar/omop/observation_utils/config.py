OBSERVATION_ID_MAPPING = {
    "pasar": ["id", "session_startdate"],
    "omop": "observation_id"
}
OBSERVATION_DATE_MAPPING = {
    "pasar": "session_startdate",
    "omop": "observation_date"
}
VALUE_AS_STRING_MAPPING = {
    "pasar": [
        ("preop.riskindex", ["h_o_ihd", "h_o_chf", "h_o_cva", "dm_on_insulin", "hypertension",
                             "history_of_osa", "loud_snoring", "daytime_tiredness", "apnoea", "cpap_use"]),
        ("preop.others", ["forget_prescribed_medications", "continence",
                          "no_of_prior_hospital_admissions", "inhalation_burns", "tbsa"]),
        ("preop.char", ["smoking_history", "pregnancy_gender",
                        "alcohol_consumption", "presence_of_malignancy"]),
        ("postop.discharge", ["days_postop"]),
        ("postop.info", ["satisfaction_at_analgesia_removal"]),
        ("postop.icu", ["resuscitation_status"]),
    ],
    "omop": "value_as_string"
}

SOURCE_TABLE_COL_NAME = "source_table"

SOURCE_TABLES = [
    "preop.riskindex",
    "preop.others",
    "preop.char",
    "postop.discharge",
    "postop.info",
    "postop.icu"
]
