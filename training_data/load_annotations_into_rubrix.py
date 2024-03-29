import rubrix as rb
import spacy
import json


def main(spacy_file, collection_name, config, doc_limit):

    nlp = spacy.load("en_core_web_sm")
    docs_bin_obj = spacy.tokens.DocBin()
    docs_bin_obj.from_disk(spacy_file)

    rb.init(api_url=config["api_url"], api_key=config["api_key"])

    rb.set_workspace(config["workspace"])

    doc_iter = docs_bin_obj.get_docs(vocab=nlp.vocab)
    record_list = []
    i = 0
    for doc_obj in doc_iter:
        labelled_entities = []
        for ent in doc_obj.ents:
            labelled_entities += [(ent.label_, ent.start_char, ent.end_char)]

        record = rb.TokenClassificationRecord(
            text=doc_obj.text,
            tokens=[token.text for token in doc_obj],
            prediction=labelled_entities,
            prediction_agent="ohnlp.custom_rules.provider",
            metadata=doc_obj.user_data
        )
        record_list += [record]

        i += 1

        if doc_limit is not None and i >= doc_limit:
            break

    rb.log(records=record_list, name=collection_name)


if __name__ == "__main__":

    with open("./rubrix_config.json") as f:
        config = json.load(f)

    # main("C:\\Users\\Janos Hajagos\\data\\covid_nlp\\20210315\\ohnlp_ohdsi_train_label_term_usage.spacy",
    #       "ohnlp_set_labelling", config, doc_limit=1000)
    #
    # main("C:\\Users\\Janos Hajagos\\data\\covid_nlp\\20210315\\ohnlp_ohdsi_train_label_positive_concepts.spacy",
    #      "ohnlp_positive_set_labelling", config, doc_limit=1000)

    main("C:\\Users\\Janos Hajagos\\data\\covid_nlp\\20210315\\ohnlp_ohdsi_train_label_positive_concepts.spacy",
         "pasc_positive_set_labelling", config, doc_limit=1000)
