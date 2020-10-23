# Create entity mapping from wikipeida page
import spacy
from spacy import displacy
from collections import Counter
#import en_core_web_sm
#import en_core_web_lg
import json
from datetime import datetime
# import pprint

# pp = pprint.PrettyPrinter(indent=4)


entity_types = ["MONEY","PERSON","EVENTS","FAC","NORP","GPE","ORG","LOC","PRODUCT", "WORK_OF_ART", "LAW", "DATE",
              "TIME", "PERCENT", "QUANTITY", "ORDINAL", "CARDINAL", "EVENT", "MISC"]


def filter_spans(spans):
    # Filter a sequence of spans so they don't contain overlaps
    get_sort_key = lambda span: (span.end - span.start, span.start)
    sorted_spans = sorted(spans, key=get_sort_key, reverse=True)
    result = []
    seen_tokens = set()
    for span in sorted_spans:
        if span.start not in seen_tokens and span.end - 1 not in seen_tokens:
            result.append(span)
            seen_tokens.update(range(span.start, span.end))
    return result


def extract_entity_relations(doc,ent_type):
    # Merge entities and noun chunks into one token
    spans = list(doc.ents) + list(doc.noun_chunks)
    spans = filter_spans(spans)
    with doc.retokenize() as retokenizer:
        for span in spans:
            retokenizer.merge(span)

    relations = []
    sentences = list(doc.sents)
    #print(str(sentences))
    for entity in filter(lambda w: w.ent_type_ == ent_type, doc):
        if entity.dep_ in ("attr", "dobj"):
            subject = [w for w in entity.head.lefts if w.dep_ == "nsubj"]
            if subject:
                subject = subject[0]               
                for sent in sentences:
                    if subject in sent: 
                        if entity.head in sent:
                            if entity in sent:
                                sentences.remove(sent)
                                relations.append((subject, entity.head, entity,sent, get_adjectives(sent,entity)))
                                
                    
        elif entity.dep_ == "pobj" and entity.head.dep_ == "prep":
            for sent in sentences:
                if entity.head.head in sent: 
                    if entity.head in sent:
                        if entity in sent:
                            sentences.remove(sent)                          
                            relations.append((entity.head.head,entity.head, entity, sent, get_adjectives(sent,entity)))
                            
                            
            
    return relations

def massage_str(curr_str):
    return curr_str.replace('\r', '').replace('\n', '').strip()

def get_adjectives(sent,subj):
    
    noun_adj_pairs = []
    for i,token in enumerate(sent):
        if token.pos_ not in ('NOUN','PROPN','PRON','VERB'):
            continue
        if not token == subj:
            continue
        all_adjs = ""
        na_json = {}
        for j in range(i+1,len(sent)):
            if sent[j].pos_ == 'ADJ':
                all_adjs += str(sent[j])+" "
            if sent[j].pos_ == 'ADV':
                all_adjs += str(sent[j])+" "
                
        na_json["subject"] = str(token)
        na_json["adjectives"] = all_adjs.strip()
        noun_adj_pairs.append(na_json) 
    return noun_adj_pairs



def analyze_whitepaper(sent,nlp):
    


    #nlp work here
    doc_nlp = nlp(sent)

    wiki_relations =""
    wiki_relations_object = {}
    wiki_relations_array = []

    for ent in entity_types:
        relations = extract_entity_relations(doc_nlp,ent)

        for r1,verb, r2, sent, adjectives in relations:
            wiki_relations_json = {}
            snippet =  str(r1.text) +" "+str(verb)+" "+ str(r2.text)
            wiki_relations_json["associationType"] = str(r2.ent_type_)
            wiki_relations_json["verb"] = massage_str(str(verb))
            wiki_relations_json["subject"] = massage_str(str(r2.text))
            wiki_relations_json["entity"] = massage_str(str(r1.text))
            wiki_relations_json["sentence"] = massage_str(str(sent))
            wiki_relations_json["snippet"] = massage_str(snippet)
            wiki_relations_json["adjectives"] = adjectives
            wiki_relations_json["collectedTime"] = str(datetime.now())
            wiki_relations_array.append(wiki_relations_json)

    #print('Done and done')
    #pp.pprint(wiki_relations_array)
    return wiki_relations_array

    