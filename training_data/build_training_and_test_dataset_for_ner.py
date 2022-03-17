import sqlalchemy as sa
import spacy
import json
import re
from spacy.tokens import DocBin
import pathlib
import random
import math
import hashlib
import pandas as pd

"""
Builds two data set for training and evaluation for NER training in SPACY

We query the OHDSI tables (note_nlp) to get annotations

We align the lexical variant using SPACY nlp

"""

if spacy.__version__.split(".")[0] != "3":
    raise (RuntimeError, "Requires version 3 of spacy library")


def main(connection_uri, schema_name, ohdsi_concept_schema, output_path, max_size, train_split, annotation_style):
    exclude_concepts = [4266367]  # Exclude influenza

    engine = sa.create_engine(connection_uri)
    with engine.connect() as connection:
        meta_data = sa.MetaData(connection, schema=schema_name)
        meta_data.reflect()

        print("Get Concept Labels")

        cursor = connection.execute(f"select concept_id, concept_name from {ohdsi_concept_schema}.concept where concept_id in (select distinct note_nlp_concept_id as concept_id from {schema_name}.note_nlp)")

        result = list(cursor)

        concept_dict = {r["concept_id"]: r["concept_name"] for r in result}

        print(f"Number of concepts found: {len(concept_dict)}")


        note_nlp_obj = meta_data.tables[schema_name + "." + "note_nlp"]
        query_obj_1 = note_nlp_obj.select().where(sa.not_(note_nlp_obj.c.note_nlp_concept_id.\
                                                        in_(exclude_concepts))).order_by(note_nlp_obj.c.note_id).limit(max_size)

        print("Executing query")
        cursor = connection.execute(query_obj_1)

        snippet_dict = {}

        re_date = re.compile("[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2} EST") # for finding dates that start a note

        print("Iterating through rows")
        i = 0
        for row in cursor:

            # Some cleaning
            snippet = row["snippet"].strip()
            re_snippet_obj = re_date.search(snippet)

            if re_snippet_obj is not None:
                snippet = snippet[re_snippet_obj.end():]
                snippet = snippet.strip()

            lexical_variant = row["lexical_variant"]
            note_nlp_concept_id = row["note_nlp_concept_id"]
            note_id = row["note_id"]
            note_nlp_id = row["note_nlp_id"]
            term_modifiers = row["term_modifiers"]
            modifier_list = term_modifiers.split(",")

            sha1 = hashlib.sha1(snippet.encode("utf8", errors="replace")).hexdigest()

            if lexical_variant in snippet:
                start_offset = snippet.index(lexical_variant)
                end_offset = start_offset + len(lexical_variant)

                match_dict = {
                             "sha1": sha1,
                             "lexical_variant": lexical_variant,
                             "note_id": note_id,
                             "note_nlp_id": note_nlp_id,
                             "term_modifiers": modifier_list,
                             "note_nlp_concept_id": note_nlp_concept_id,
                             "offsets": (start_offset, end_offset),
                             }

                if snippet in snippet_dict:
                    snippet_dict[snippet] += [match_dict]
                else:
                    snippet_dict[snippet] = [match_dict]

            if max_size is not None and i == max_size:
               break

            if i > 0 and i % 1000 == 0:
                print(f"Number of fragments processed: {i}")

            i += 1

        # Build annotations
        print(f"Build annotations for {len(snippet_dict)} distinct text fragments")

        nlp = spacy.load("en_core_web_sm")
        doc_list = []
        meta_data_list = []
        i = 0
        for snippet in snippet_dict:

            sha1 = hashlib.sha1(snippet.encode("utf8", errors="replace")).hexdigest()
            doc = nlp(snippet)
            doc.user_data["sha1"] = sha1

            variants_found = []
            spans = []
            for variant_dict in snippet_dict[snippet]:
                variant = variant_dict["lexical_variant"]
                meta_data_list += [variant_dict]
                start_position, end_position = variant_dict["offsets"]
                term_modifiers = variant_dict["term_modifiers"]
                concept_id = variant_dict["note_nlp_concept_id"]

                annotation_label = term_modifiers[0].split("=")[1]
                if variant not in variants_found and annotation_label is not None:
                    variants_found += [variant]
                    if annotation_style == "label_term_usage":
                        spans += [doc.char_span(start_position, end_position, label=annotation_label, alignment_mode="expand")]
                    elif annotation_style == "label_positive_concepts":
                        if annotation_label == "Positive":
                            if concept_id in concept_dict:
                                concept_name = concept_dict[concept_id] + "|" + str(concept_id)
                            else:
                                concept_name = str(concept_id)
                            spans += [doc.char_span(start_position, end_position, label=concept_name,
                                                    alignment_mode="expand")]
                    elif annotation_style == "label_negated_concepts":
                        if annotation_label == "Negated":
                            if concept_id in concept_dict:
                                concept_name = concept_dict[concept_id] + "|" + str(concept_id)
                            else:
                                concept_name = str(concept_id)
                            spans += [doc.char_span(start_position, end_position, label=concept_name,
                                                    alignment_mode="expand")]

            try:
                doc.set_ents(spans)
                doc_list += [doc]
            except ValueError:
                print("Unable to set annotations")
                print(snippet)

            if i > 0 and i % 1000 == 0:
                print(f"Processed {i} snippets")

            i += 1

        p_output_path = pathlib.Path(output_path)

        meta_data_df = pd.DataFrame(meta_data_list)
        meta_data_df.to_csv(p_output_path / "meta_data.csv", index=False)

        full_set_path = p_output_path / f"ohnlp_full_set_{annotation_style}.spacy"
        print(f"Writing: '{full_set_path}'")
        full_set_obj = DocBin(docs=doc_list, store_user_data=True)
        full_set_obj.to_disk(full_set_path)

        # Shuffle list to randomize
        random.shuffle(doc_list)
        number_of_documents = len(doc_list)

        test_size = int(math.floor(number_of_documents * train_split))
        training_size = number_of_documents - test_size

        training_corpora_path = p_output_path / f"ohnlp_ohdsi_train_{annotation_style}.spacy"
        print(f"Writing training set (n={training_size}) to: '{training_corpora_path}'")

        train_doc_bin_obj = DocBin(docs=doc_list[0:training_size], store_user_data=True)
        train_doc_bin_obj.to_disk(training_corpora_path)

        test_doc_bin_obj = DocBin(docs=doc_list[training_size:], store_user_data=True)
        testing_corpora_path = p_output_path / f"ohnlp_ohdsi_test_{annotation_style}.spacy"
        print(f"Writing test set (n={test_size}) to '{testing_corpora_path}'")
        test_doc_bin_obj.to_disk(testing_corpora_path)


if __name__ == "__main__":

    import argparse
    arg_parse_obj = argparse.ArgumentParser(description="Build a training set and a test set for named entity recognition for OHNLP pipeline to OHDSI")

    arg_parse_obj.add_argument("-c", "--config-json-file-name", dest="config_json_file_name", default="./config.json",
                               help="JSON dictionary with following keys: \"connection_uri\", \"schema\", \"data_directory\""
                               )
    arg_parse_obj.add_argument("-t", "--test-size-split", dest="test_size_split", default="0.3",
                               help="Fractional split of training size must be between 0 and 1")
    arg_parse_obj.add_argument("-m", "--maximum-number-of-documents", dest="maximum_number_of_documents",
                               help="Maximum number of documents; default is no restriction", default=10000)

    arg_parse_obj.add_argument("-a", "--annotation_style", dest="annotation_style", default="label_positive_concepts",
                               help="Two choices: label_term_usage or label_positive_concepts")

    arg_obj = arg_parse_obj.parse_args()

    test_size = arg_obj.test_size_split
    try:
        float_test_size = float(test_size)
    except ValueError:
        raise (RuntimeError, "Invalid number")

    if float_test_size >= 0.0 and float_test_size <= 1.0:
        pass
    else:
        raise (RuntimeError, "Fractional test size should between 0 and 1")

    maximum_size = arg_obj.maximum_number_of_documents
    if maximum_size is not None:
        try:
            int_maximum_size = int(maximum_size)
        except ValueError:
            raise (RuntimeError, "Not an integer")
    else:
        int_maximum_size = None

    with open(arg_obj.config_json_file_name) as f:
        config = json.load(f)

    main(config["connection_uri"], config["schema"], config["ohdsi_schema"], config["data_directory"], int_maximum_size,
         float_test_size, arg_obj.annotation_style)
