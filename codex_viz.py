import json

import pandas as pd
import streamlit as st

import graphviz


def make_ent_rel_graph(row,graph):
    graph.edge(row["source"], row["target"], label=row["edge"])




def cluster_graph(answers,ents,rels,codexkg):

    clusters = answers["answers"]["clusters"]
    data = answers["answers"]["ent_map"]


    concepts = list(data.keys())


    # st.write(codexkg.entity_map)
    # st.write(codexkg.rel_map)


    for cluster in clusters:
        graph = graphviz.Digraph(f"Cluster {cluster}")
        


        for concept in concepts:


            curr_cluster_data = data[concept]["data"][cluster]

            if concept in ents:
                counter = 0
                for datum in curr_cluster_data:

                    graph.node(f"{concept}_{counter}",shape="box", style='filled', color='#b19cd9')

                    for val in datum:
                        #st.write(datum[val])

                        attr_string = f"{concept}_{counter}_{val}_{datum[val]}"
                        
                        graph.node(attr_string, style='filled', color='#add8e6')

                        graph.edge(f"{concept}_{counter}",attr_string, label="has")
                    

                    counter += 1
                

            else:
                counter = 0
                for datum in curr_cluster_data:
                    graph.node(f"{concept}_{counter}",shape="diamond", style='filled', color='#90ee90')

                    #codex_details = eval(datum["codex_details"])



                    curr_rel = codexkg.rel_map[concept]
                    rel1_label = curr_rel["rel1"]["role"]
                    rel2_label = curr_rel["rel2"]["role"]


                    rel1_ent = curr_rel["rel1"]["entity"]
                    rel2_ent = curr_rel["rel2"]["entity"]



                    graph.edge(f"{concept}_{counter}", f"{rel1_ent}_{counter}", label=rel1_label)
                    graph.edge(f"{concept}_{counter}", f"{rel2_ent}_{counter}", label=rel2_label)

                    for col in datum:
                        if col =="codex_details":
                            continue

                        attr_string = f"{concept}_{counter}_{col}_{datum[col]}"

                        graph.edge(f"{concept}_{counter}", attr_string, label="has")
                        graph.node(attr_string, style='filled', color='#add8e6')

            




        st.graphviz_chart(graph)



def ent_rel_graph(ents,rels,keyspace):


    graph = graphviz.Digraph(keyspace)    

    #for all ents get the attrs
    #st.write(rels)

    ent_keys = list(ents.keys())

    for key in ent_keys:
        graph.node(key,shape="box", style='filled', color='#b19cd9')


        for attr in ents[key]["cols"]:
            graph.edge(key, attr, label="has")
            graph.node(attr, style='filled', color='#add8e6')

    rel_keys = list(rels.keys())


    for rel in rel_keys:
        graph.node(rel,shape="diamond", style='filled', color='#90ee90')

        #check rel1
        rel1_ent = rels[rel]["rel1"]["entity"]
        rel1_label = rels[rel]["rel1"]["role"]
        graph.edge(rel, rel1_ent, label=rel1_label)

        #check rel2
        rel2_ent = rels[rel]["rel2"]["entity"]
        rel2_label = rels[rel]["rel2"]["role"]
        graph.edge(rel, rel2_ent, label=rel2_label)


        #check attrs
        for col in rels[rel]["cols"]:
            if col =="codex_details":
                continue
            graph.edge(rel, col, label="has")
            graph.node(col, style='filled', color='#add8e6')


    if len(ent_keys) > 0:
        graph.attr(fontsize='20')
        graph.attr(label=r'\n\nEntity Relation Diagram\ for '+keyspace)
        st.graphviz_chart(graph)






