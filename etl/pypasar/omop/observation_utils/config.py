import os

class ObservationMappingConfig:
    # # # One to one mapping START # # #
    observation_id_mapping = {
        "pasar": ["id", "session_startdate"],
        "omop": "observation_id",
    }

    person_id_mapping = {"pasar": "anon_case_no", "omop": "person_id"}

    observation_date_mapping = {
        "pasar": "session_startdate",
        "omop": "observation_date",
    }

    observation_type_concept_id_mapping = {
        "pasar": "",
        "omop": "observation_type_concept_id",
    }

    visit_occurrence_id_mapping = {
        "pasar": "session_id", "omop": "visit_occurrence_id", "joinpasaromop": "omop_visit_occurrence_id"}

    value_as_concept_id_mapping = {
        "pasar": "allergy_information",
        "omop": "value_as_concept_id",
    }
    # # # One to one mapping END # # #

    # # # # EAV mapping config START # # #
    value_as_string_mapping = {
        "pasar": {
            "preop.riskindex": [
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
            "preop.others": [
                "forget_prescribed_medications",
                "continence",
                "no_of_prior_hospital_admissions",
                "inhalation_burns",
                "tbsa",
            ],
            "preop.char": [
                "smoking_history",
                "pregnancy_gender",
                "alcohol_consumption",
                "presence_of_malignancy",
            ],
            "postop.discharge": ["days_postop"],
            "postop.info": ["satisfaction_at_analgesia_removal"],
            "postop.icu": ["resuscitation_status"],
        },
        "omop": "value_as_string",
    }

    observation_source_value_mapping = {
        "pasar": {
            "preop.riskindex": [
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
            "preop.others": [
                "forget_prescribed_medications",
                "no_of_prior_hospital_admissions",
                "continence",
                "inhalation_burns",
                "tbsa",
            ],
            "preop.char": [
                "smoking_history",
                "alcohol_consumption",
                "pregnancy_gender",
                "presence_of_malignancy",
                "allergy_information",
                "physical_general",
                "physical_cardio",
                "physical_respiratory",
            ],
            "postop.discharge": ["days_postop"],
            "postop.info": [
                "postop_patient_satisfaction",
                "satisfaction_at_analgesia_removal",
            ],
            "postop.icu": ["resuscitation_status"],
        },
        "omop": "observation_source_value",
    }
    value_source_value_mapping = {
        "pasar": {
            "preop.riskindex": [
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
            "preop.others": [
                "forget_prescribed_medications",
                "no_of_prior_hospital_admissions",
                "continence",
                "inhalation_burns",
                "tbsa",
            ],
            "preop.char": [
                "allergy_information",
                "smoking_history",
                "alcohol_consumption",
                "pregnancy_gender",
                "presence_of_malignancy",
                "physical_general",
                "physical_cardio",
                "physical_respiratory",
            ],
            "postop.discharge": ["days_postop"],
            "postop.info": [
                "postop_patient_satisfaction",
                "satisfaction_at_analgesia_removal",
            ],
            "postop.icu": ["resuscitation_status"],
        },
        "omop": "value_source_value",
    }
    observation_concept_id_mapping = {
        "pasar": {
            "preop.riskindex": [
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
            "preop.others": [
                "forget_prescribed_medications",
                "no_of_prior_hospital_admissions",
                "continence",
            ],
            "preop.char": [
                "smoking_history",
                "alcohol_consumption",
                "pregnancy_gender",
                "presence_of_malignancy",
                "allergy_information",
                "physical_general",
                "physical_cardio",
                "physical_respiratory",
            ],
            "postop.discharge": ["days_postop"],
            "postop.info": [
                "postop_patient_satisfaction",
                "satisfaction_at_analgesia_removal",
            ],
            "postop.icu": ["resuscitation_status"],
        },
        "omop": "observation_concept_id",
    }

    value_as_number_mapping = {
        "pasar": {"postop.info": ["postop_patient_satisfaction"]},
        "omop": "value_as_number",
    }
    # # # # EAV mapping config END # # #

    observation_concept_id_specific_config = {
        "preop.riskindex": {
            "high_risk_op": {"use_hardcoded_value": 0},
            "h_o_ihd": {"use_hardcoded_value": 0},
            "h_o_chf": {"use_hardcoded_value": 0},
            "h_o_cva": {"use_hardcoded_value": 0},
            "dm_on_insulin": {"use_hardcoded_value": 0},
            "hypertension": {"use_hardcoded_value": 4220915},
            "history_of_osa": {"use_hardcoded_value": 0},
            "loud_snoring": {"use_hardcoded_value": 35810206},
            "daytime_tiredness": {"use_hardcoded_value": 0},
            "apnoea": {"use_hardcoded_value": 313459},
            "cpap_use": {"use_hardcoded_value": 0},
            "history_of_hypertension": {"use_hardcoded_value": 0}, # Source concept ID - 3374101
            "history_of_motion_sickness": {"use_hardcoded_value": 0},
            "postop_nausea_smoking_history": {"use_hardcoded_value": 0},
        },
        "preop.others": {
            "forget_prescribed_medications": {"use_hardcoded_value": 0}, # Source concept ID - 45921872
            "no_of_prior_hospital_admissions": {"use_hardcoded_value": 0},
            "continence": {"use_hardcoded_value": 0},
        },
        "preop.char": {
            "smoking_history": {"use_hardcoded_value": 1340204},    # Source concept ID - 46274087
            "alcohol_consumption": {"use_hardcoded_value": 619635}, # Source concept ID - 45454785
            "pregnancy_gender": {"use_hardcoded_value": 4199558},
            "presence_of_malignancy": {"use_hardcoded_value": 0},
            "allergy_information": {"use_source_to_concept_mapping": True},
            "physical_general": {"use_hardcoded_value": 4093837},
            "physical_cardio": {"use_hardcoded_value": 4154948},
            "physical_respiratory": {"use_hardcoded_value": 4156044},
        },
        "postop.discharge": {"days_postop": {"use_hardcoded_value": 0}},
        "postop.info": {
            "postop_patient_satisfaction": {"use_hardcoded_value": 0},
            "satisfaction_at_analgesia_removal": {"use_hardcoded_value": 0},
        },
        "postop.icu": {"resuscitation_status": {"use_hardcoded_value": 4127294}},
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

CHUNK_SIZE = int(os.getenv("PROCESSING_BATCH_SIZE"))
