Download MT Samples CSV:

https://www.kaggle.com/tboyle10/medicaltranscriptions

1) Load into a staging table in a PostGreSQL schema and table
   
1) Create OHDSI note and note_nlp tables

```sql
drop table if exists note;

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE note
(
  note_id						    INTEGER			  NOT NULL ,
  person_id						  BIGINT			  NOT NULL ,
  note_date						  DATE			    NOT NULL ,
  note_datetime					TIMESTAMP		  NULL ,
  note_type_concept_id	INTEGER			  NOT NULL ,
  note_class_concept_id INTEGER			  NOT NULL ,
  note_title					  VARCHAR(250)	NULL ,
  note_text						  TEXT  NULL ,
  encoding_concept_id		INTEGER			  NOT NULL ,
  language_concept_id		INTEGER			  NOT NULL ,
  provider_id					  INTEGER			  NULL ,
  visit_occurrence_id		BIGINT			  NULL ,
  visit_detail_id       INTEGER       NULL ,
  note_source_value			VARCHAR(50)		NULL
)
;

drop table if exists note_nlp;
CREATE TABLE note_nlp
(
  note_nlp_id					        INTEGER			  NOT NULL ,
  note_id						          INTEGER			  NOT NULL ,
  section_concept_id			    INTEGER			  NULL ,
  snippet						          VARCHAR(5000)	NULL ,
  "offset"					          VARCHAR(250)	NULL ,
  lexical_variant				      VARCHAR(250)	NOT NULL ,
  note_nlp_concept_id			    INTEGER			  NULL ,
  note_nlp_source_concept_id  INTEGER			  NULL ,
  nlp_system					        VARCHAR(250)	NULL ,
  nlp_date						        DATE			    NOT NULL ,
  nlp_datetime					      TIMESTAMP		  NULL ,
  term_exists					        VARCHAR(1)		NULL ,
  term_temporal					      VARCHAR(50)		NULL ,
  term_modifiers				      VARCHAR(2000)	NULL
)
;
```

Create sequence for auto-incrementing id:

```
--We need a sequence when nlp matches are added to note_nlp
create sequence seq_note_nlp;

ALTER TABLE note_nlp ALTER COLUMN note_nlp_id SET DEFAULT nextval('seq_note_nlp');
```


Example for discharge summaries:
```sql
insert into note ( note_id, person_id, note_date, note_datetime, note_type_concept_id,
            note_class_concept_id, note_title, note_text, encoding_concept_id, language_concept_id,
            provider_id, visit_occurrence_id, visit_detail_id, note_source_value)
select id, --note_id
       cast(patient_id as bigint), --person_id
       document_datetime::date, --note_date
       document_datetime, -- note_datetime
       32817, -- EHR note_type_concept_id
       706531, --Discharge Summary note_class_concept_id
       document_type, --note_title
       file_text, --note_text
       32678, --encoding_concept_id
       4180186, --english language_Type
       null, --provider_id
       cast(encounter_id as bigint), -- visit_occurrence_id
       null, --visit_detail_id
       mapped_document_type --note_source_value
from discharge_summaries ;
```

For information on fields see:

https://ohdsi.github.io/CommonDataModel/cdm531.html#NOTE