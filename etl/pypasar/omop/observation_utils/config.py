class ObservationMappingConfig:
    observation_id_mapping = {
        "pasar": ["id", "session_startdate"],
        "omop": "observation_id",
    }

    person_id_mapping = {"pasar": "anon_case_no", "omop": "person_id"}

    observation_date_mapping = {
        "pasar": "session_startdate",
        "omop": "observation_date",
    }
    value_as_string_mapping = {
        "pasar": [
            (
                "preop.riskindex",
                [
                    "h_o_ihd",
                    "h_o_chf",
                    "h_o_cva",
                    "dm_on_insulin",
                    "hypertension",
                    "history_of_osa",
                    "loud_snoring",
                    "daytime_tiredness",
                    "apnoea",
                    "cpap_use",
                ],
            ),
            (
                "preop.others",
                [
                    "forget_prescribed_medications",
                    "continence",
                    "no_of_prior_hospital_admissions",
                    "inhalation_burns",
                    "tbsa",
                ],
            ),
            (
                "preop.char",
                [
                    "smoking_history",
                    "pregnancy_gender",
                    "alcohol_consumption",
                    "presence_of_malignancy",
                ],
            ),
            ("postop.discharge", ["days_postop"]),
            ("postop.info", ["satisfaction_at_analgesia_removal"]),
            ("postop.icu", ["resuscitation_status"]),
        ],
        "omop": "value_as_string",
    }

    observation_type_concept_id_mapping = {
        "pasar": "",
        "omop": "observation_type_concept_id",
    }

    visit_occurrence_id_mapping = {"pasar": "session_id", "omop": "visit_occurrence_id"}

    observation_source_value_mapping = {
        "pasar": [
            (
                "preop.riskindex",
                [
                    "high_risk_op",
                    "h_o_ihd",
                    "h_o_chf",
                    "h_o_cva",
                    "dm_on_insulin",
                    "hypertension",
                    "history_of_osa",
                    "loud_snoring",
                    "daytime_tiredness",
                    "apnoea",
                    "cpap_use",
                    "history_of_hypertension",
                    "history_of_motion_sickness",
                    "postop_nausea_smoking_history",
                ],
            ),
            (
                "preop.others",
                [
                    "forget_prescribed_medications",
                    "no_of_prior_hospital_admissions",
                    "continence",
                    "inhalation_burns",
                    "tbsa",
                ],
            ),
            (
                "preop.char",
                [
                    "smoking_history",
                    "alcohol_consumption",
                    "pregnancy_gender",
                    "presence_of_malignancy",
                    "allergy_information",
                    "physical_general",
                    "physical_cardio",
                    "physical_respiratory",
                ],
            ),
            ("postop.discharge", ["days_postop"]),
            (
                "postop.info",
                ["postop_patient_satisfaction", "satisfaction_at_analgesia_removal"],
            ),
            ("postop.icu", ["resuscitation_status"]),
        ],
        "omop": "observation_source_value",
    }
    value_source_value_mapping = {
        "pasar": [
            (
                "preop.riskindex",
                [
                    "high_risk_op",
                    "h_o_ihd",
                    "h_o_chf",
                    "h_o_cva",
                    "dm_on_insulin",
                    "hypertension",
                    "history_of_osa",
                    "loud_snoring",
                    "daytime_tiredness",
                    "apnoea",
                    "cpap_use",
                    "history_of_hypertension",
                    "history_of_motion_sickness",
                    "postop_nausea_smoking_history",
                ],
            ),
            (
                "preop.others",
                [
                    "forget_prescribed_medications",
                    "no_of_prior_hospital_admissions",
                    "continence",
                    "inhalation_burns",
                    "tbsa",
                ],
            ),
            (
                "preop.char",
                [
                    "allergy_information",
                    "smoking_history",
                    "alcohol_consumption",
                    "pregnancy_gender",
                    "presence_of_malignancy",
                    "physical_general",
                    "physical_cardio",
                    "physical_respiratory",
                ],
            ),
            ("postop.discharge", ["days_postop"]),
            (
                "postop.info",
                ["postop_patient_satisfaction", "satisfaction_at_analgesia_removal"],
            ),
            ("postop.icu", ["resuscitation_status"]),
        ],
        "omop": "value_source_value",
    }

    value_as_number_mapping = {
        "pasar": [("postop.info", "postop_patient_satisfaction")],
        "omop": "value_as_number",
    }

    value_as_concept_id_mapping = {
        "pasar": "allergy_information",
        "omop": "value_as_concept_id",
    }


SOURCE_TABLE_COL_NAME = "source_table"

SOURCE_TABLES = [
    "preop.riskindex",
    "preop.others",
    "preop.char",
    "postop.discharge",
    "postop.info",
    "postop.icu",
]
