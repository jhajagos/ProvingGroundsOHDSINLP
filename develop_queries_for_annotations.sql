-- Develop queries for building annotation spreadsheet for COVID positive patients

select * from sbm_covid19_documents.note limit 10;

select count(*) as n_r, count(distinct visit_occurrence_id) as n_visits,
      count(distinct person_id) as n from sbm_covid19_documents.note ;
--438258,79849,18284



select count(*) as n_r, count(distinct visit_occurrence_id) as n_visits, count(distinct person_id) as n,
       c.concept_name as note_class_concept_name,
       n.note_class_concept_id
    from sbm_covid19_documents.note n join sbm_covid19_hi_cdm_build.concept c
            on c.concept_id = n.note_class_concept_id
    group by c.concept_name, n.note_class_concept_id order by c.concept_name
;

/*
15923,8494,5777,Admission evaluation
9164,8022,5371,Discharge summary
55703,17160,10352,Emergency medicine
322760,70395,15208,Progress
34708,12124,7919,Radiology
*/


--Find documents which have a concept match
select count(*) as n_r, count(distinct n.note_id) as n_notes,
       count(distinct visit_occurrence_id) as n_visits, count(distinct person_id) as n,
       c1.concept_name as note_class_concept_name
    from sbm_covid19_documents.note n
        join sbm_covid19_hi_cdm_build.concept c1
            on c1.concept_id = n.note_class_concept_id
        join sbm_covid19_documents.note_nlp nl on n.note_id = nl.note_id
    group by c1.concept_name order by c1.concept_name
;

/*
85345,10202,7686,5269,Admission evaluation
40371,7015,6589,4660,Discharge summary
211826,25135,16623,10146,Emergency medicine
 */

 select * from sbm_covid19_documents.note_nlp limit 10;

--Find all terms
 select count(*) as n_r, count(distinct n.note_id) as n_notes,
       count(distinct visit_occurrence_id) as n_visits, count(distinct person_id) as n,
       c1.concept_name as note_class_concept_name,
       c2.concept_id as note_nlp_concept_id, c2.concept_name as note_nlp_concept_name
    from sbm_covid19_documents.note n
        join sbm_covid19_hi_cdm_build.concept c1
            on c1.concept_id = n.note_class_concept_id
        join sbm_covid19_documents.note_nlp nl on n.note_id = nl.note_id
        join sbm_covid19_hi_cdm_build.concept c2 on c2.concept_id = nl.note_nlp_concept_id
    group by c1.concept_name, c2.concept_name, c2.concept_id order by c1.concept_name, c2.concept_name;

 select count(*) as n_r, count(distinct n.note_id) as n_notes,
       count(distinct visit_occurrence_id) as n_visits, count(distinct person_id) as n,
       c1.concept_name as note_class_concept_name,
       c2.concept_id as note_nlp_concept_id, c2.concept_name as note_nlp_concept_name
    from sbm_covid19_documents.note n
        join sbm_covid19_hi_cdm_build.concept c1
            on c1.concept_id = n.note_class_concept_id
        join sbm_covid19_documents.note_nlp nl on n.note_id = nl.note_id
        join sbm_covid19_hi_cdm_build.concept c2 on c2.concept_id = nl.note_nlp_concept_id
        where left(term_modifiers, 18) =  'certainty=Positive'
    group by c1.concept_name, c2.concept_name, c2.concept_id order by c1.concept_name, c2.concept_name;

--Query to find selected visits that are positive
select v.visit_occurrence_id, v.encounter_number, v.covid19_status, cv.*
    from sbm_covid19_analytics_build.critical_covid_visits_linked_to_hi v
        join sbm_covid19_analytics_build.checked_visit_index cv on cv.visit_occurrence_id = v.visit_occurrence_id
            where encounter_number is not null and v.covid19_status = 'positive'
limit 10;

--For Covid positive patients selects the visit and annotations
 with selected_positive_visits as (
    select cast(v.encounter_number as bigint) as encounter_number,
           v.visit_start_datetime,
           v.visit_end_datetime,
           v.visit_concept_name
    from sbm_covid19_analytics_build.critical_covid_visits_linked_to_hi v
        join sbm_covid19_analytics_build.checked_visit_index cv on cv.visit_occurrence_id = v.visit_occurrence_id
            where encounter_number is not null and encounter_number ~ '^[0-9]+' and v.covid19_status = 'positive')
 select * from (
                   select distinct sv.*,
                                   n.note_id,
                                   n.person_id                                          as mrn,
                                   c1.concept_name                                      as note_class_concept_name,
                                   note_class_concept_id,
                                   note_nlp_concept_id,
                                   note_nlp_concept_name,
                                   case when x.note_id is not null then 1 else null end as counter
                   from selected_positive_visits sv
                            join sbm_covid19_documents.note n on n.visit_occurrence_id = sv.encounter_number
                            join sbm_covid19_hi_cdm_build.concept c1
                                 on c1.concept_id = n.note_class_concept_id
                            left outer join
                            (select nl.note_id,
                                    c2.concept_id   as note_nlp_concept_id,
                                    c2.concept_name as note_nlp_concept_name
                             from sbm_covid19_documents.note_nlp nl
                                      join sbm_covid19_hi_cdm_build.concept c2 on c2.concept_id = nl.note_nlp_concept_id
                             where left(term_modifiers, 18) = 'certainty=Positive'
                               and c2.concept_name != 'Influenza') x
                        on x.note_id = n.note_id
               ) t
            order by encounter_number, note_class_concept_name, note_nlp_concept_id
;





select * from sbm_covid19_analytics_build.critical_covid_manual_chart_review mcr
join
(
    select distinct cast(visit_occurrence_id as varchar(36)) as encounter_number
        from sbm_covid19_documents.note n join sbm_covid19_hi_cdm_build.concept c
                on c.concept_id = n.note_class_concept_id
        where c.concept_name in ('Emergency medicine', 'Admission evaluation', 'Discharge summary')
) t on t.encounter_number = mcr.encounter_number
join
(
select v.visit_occurrence_id from sbm_covid19_analytics_build.critical_covid_visits_linked_to_hi v
        join sbm_covid19_analytics_build.checked_visit_index cv on cv.visit_occurrence_id = v.visit_occurrence_id
            where encounter_number is not null and v.covid19_status = 'positive'
) tt on tt.visit_occurrence_id = mcr.visit_occurrence_id
where visit_start_datetime < load_time
order by visit_start_datetime desc
;