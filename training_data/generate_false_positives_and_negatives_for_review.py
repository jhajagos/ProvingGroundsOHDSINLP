import pandas as pd
import spacy
import json
import pathlib


def main(config, concept_names):

    p_data_directory = pathlib.Path(config["data_directory"])

    matched_df = pd.read_csv(p_data_directory / "merged_nlp_with_manual_chart_review_with_comp.csv")
    pos_cross_walk_df = pd.read_csv(p_data_directory / "pos_matched_and_filtered_notes_with_matched_concepts.csv")
    meta_data_df = pd.read_csv(p_data_directory / "meta_data.csv")

    neg_cross_walk_df = pd.read_csv(p_data_directory / "neg_matched_and_filtered_notes_with_matched_concepts.csv")

    # Positive
    pos_spacy_annotated_file = p_data_directory / "ohnlp_full_set_label_positive_concepts.spacy"
    neg_spacy_annotated_file = p_data_directory / "ohnlp_full_set_label_negated_concepts.spacy"

    nlp = spacy.load("en_core_web_sm")
    pos_doc_bin = spacy.tokens.DocBin()
    pos_doc_bin.from_disk(pos_spacy_annotated_file)
    pos_iter_docs = pos_doc_bin.get_docs(vocab=nlp.vocab)

    pos_doc_list = list(pos_iter_docs)

    pos_doc_position_with_sha1_list_dict = [{"position": i, "sha1": pos_doc_list[i].user_data["sha1"], "text": pos_doc_list[i].text}
                                        for i in range(len(pos_doc_list))]

    pos_doc_position_with_sha1_df = pd.DataFrame(pos_doc_position_with_sha1_list_dict)

    # Negated
    neg_doc_bin = spacy.tokens.DocBin()
    neg_doc_bin.from_disk(neg_spacy_annotated_file)
    neg_iter_docs = neg_doc_bin.get_docs(vocab=nlp.vocab)

    neg_doc_list = list(neg_iter_docs)

    neg_doc_position_with_sha1_list_dict = [
        {"position": i, "sha1": neg_doc_list[i].user_data["sha1"], "text": neg_doc_list[i].text}
        for i in range(len(neg_doc_list))]

    neg_doc_position_with_sha1_df = pd.DataFrame(neg_doc_position_with_sha1_list_dict)

    i = 0
    for concept_name in concept_names:

        print(f"Processing '{concept_name}'")

        # Positives
        false_positives_df = matched_df[matched_df[f"note_nlp_concept_name={concept_name}|fp"] == 1]

        fp_with_crosswalk_df = false_positives_df[["encounter_number"]].merge(
            pos_cross_walk_df[pos_cross_walk_df.note_nlp_concept_name == concept_name], on="encounter_number")

        fp_with_crosswalk_sha1_df = fp_with_crosswalk_df.merge(meta_data_df, on="note_nlp_id")
        fp_with_crosswalk_sha1_pos_df = fp_with_crosswalk_sha1_df.merge(pos_doc_position_with_sha1_df, on="sha1")
        fp_with_crosswalk_sha1_pos_df = fp_with_crosswalk_sha1_pos_df.sort_values(["encounter_number", "note_class_concept_id", "note_id_x", "sha1"])

        if i == 0:
            combined_fp_df = fp_with_crosswalk_sha1_pos_df.copy()
        else:
            combined_fp_df = pd.concat([combined_fp_df, fp_with_crosswalk_sha1_pos_df])


        # Negated terms
        false_negatives_df = matched_df[matched_df[f"note_nlp_concept_name={concept_name}|fn"] == 1]

        fn_with_crosswalk_df = false_negatives_df[["encounter_number"]].merge(
            neg_cross_walk_df[neg_cross_walk_df.note_nlp_concept_name == concept_name], on="encounter_number")

        print(fn_with_crosswalk_df)

        fn_with_crosswalk_sha1_df = fn_with_crosswalk_df.merge(meta_data_df, on="note_nlp_id")
        fn_with_crosswalk_sha1_neg_df = fn_with_crosswalk_sha1_df.merge(neg_doc_position_with_sha1_df, on="sha1")
        fn_with_crosswalk_sha1_neg_df = fn_with_crosswalk_sha1_neg_df.sort_values(["encounter_number", "note_class_concept_id", "note_id_x", "sha1"])

        if i == 0:
            combined_fn_df = fn_with_crosswalk_sha1_neg_df.copy()
        else:
            combined_fn_df = pd.concat([combined_fn_df, fn_with_crosswalk_sha1_neg_df])

        i += 1

    def clean_text_for_excel(text):

        if '-' == str(text)[0]:
            return "'" + str(text) + "'"
        else:
            return str(text)

    combined_fp_df["text"] = combined_fp_df["text"].map(clean_text_for_excel)
    combined_fp_df.to_csv(p_data_directory / "false_positive_examples.csv", index=False)

    combined_fn_df["text"] = combined_fn_df["text"].map(clean_text_for_excel)
    combined_fn_df.to_csv(p_data_directory / "false_negative_examples.csv", index=False)


if __name__ == "__main__":

    with open("config.json") as f:
        config = json.load(f)

    concept_names_to_check = ["Dry cough", "Dyspnea", "Diarrhea", "Abdominal pain", "Fever"]

    main(config, concept_names_to_check)