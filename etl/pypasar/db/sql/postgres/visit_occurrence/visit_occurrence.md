# Table name: Visit_occurrence

![ODSHI APAC 2024 _ ETL Team (4)](https://github.com/user-attachments/assets/40ae4392-7cd8-48c5-be6e-f8d87ee50f87)

| | Destination field | Source field | Logic | Comment field |
| --- | --- | --- | --- | --- |
| 1 | visit_occurrence_id | session_id <br> session_startdate | Use `session_id` as `visit_occurrence_id` since it is already an INTEGER. <br><br> ℹ️ Add a number suffix to ensure it is unique | **Neither field is unique, so some rows might be missing during processing. |
| 2 | person_id | anon_case_no | Join with person.person_source_value for the `person_id` | |
| 3 | visit_concept_id | admission_type | Inpatient -> 9201 <br> Same Day Admission (SDA) -> 9201 <br> Short Stay Ward (SSW) -> 9201 <br> Day Surgery (DS) -> 9202 | [Accepted Concepts](https://athena.ohdsi.org/search-terms/terms?domain=Visit&standardConcept=Standard&page=1&pageSize=15&query=) |
| 4 | visit_start_date | session_startdate | | |
| 5 | visit_start_datetime | <i style="color:gray;">NULL</i> | <i style="color:gray;">NULL</i> | |
| 6 | visit_end_date | session_enddate | | |
| 7 | visit_end_datetime | <i style="color:gray;">NULL</i> | <i style="color:gray;">NULL</i> | |
| 8 | visit_type_concept_id | <i style="color:gray;">NULL</i> | PASAR Data is Registry. <br> Registry -> 32879 | |
| 9 | provider_id | <i style="color:gray;">NULL</i> | <i style="color:gray;">NULL</i> | |
| 10 | care_site_id | insitution_code | Join with care_site.care_site_source_value for the `care_site_id` | **Only put id for SGH here |
| 11 | visit_source_value | admission_type | | It has a value like: <br> Inpatient Day Surgery (DS) <br> Same Day Admission (SDA) |
| 12 | visit_source_concept_id | <i style="color:gray;">NULL</i> | <i style="color:gray;">NULL</i> | |
| 13 | admitted_from_concept_id | <i style="color:gray;">NULL</i> | <i style="color:gray;">NULL</i> | |
| 14 | admitted_from_source_value | <i style="color:gray;">NULL</i> | <i style="color:gray;">NULL</i> | |
| 15 | discharged_to_concept_id | <i style="color:gray;">NULL</i> | <i style="color:gray;">NULL</i> | |
| 16 | discharged_to_source_value | external_hospital_code | | |
| 17 | preceding_visit_occurrence_id | <i style="color:gray;">NULL</i> | <i style="color:gray;">NULL</i> | |