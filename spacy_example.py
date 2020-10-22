from nltk.stem.wordnet import WordNetLemmatizer
from spacy.lang.en import English

SUBJECTS = ["nsubj", "nsubjpass", "csubj", "csubjpass", "agent", "expl"]
OBJECTS = ["dobj", "dative", "attr", "oprd"]
ADJECTIVES = [
    "acomp",
    "advcl",
    "advmod",
    "amod",
    "appos",
    "nn",
    "nmod",
    "ccomp",
    "complm",
    "hmod",
    "infmod",
    "xcomp",
    "rcmod",
    "poss",
    " possessive",
]
COMPOUNDS = ["compound"]
PREPOSITIONS = ["prep"]


def getSubsFromConjunctions(subs):
    moreSubs = []
    for sub in subs:
        # rights is a generator
        rights = list(sub.rights)
        rightDeps = {tok.lower_ for tok in rights}
        if "and" in rightDeps:
            moreSubs.extend(
                [tok for tok in rights if tok.dep_ in SUBJECTS or tok.pos_ == "NOUN"]
            )
            if len(moreSubs) > 0:
                moreSubs.extend(getSubsFromConjunctions(moreSubs))
    return moreSubs


def getObjsFromConjunctions(objs):
    moreObjs = []
    for obj in objs:
        # rights is a generator
        rights = list(obj.rights)
        rightDeps = {tok.lower_ for tok in rights}
        if "and" in rightDeps:
            moreObjs.extend(
                [tok for tok in rights if tok.dep_ in OBJECTS or tok.pos_ == "NOUN"]
            )
            if len(moreObjs) > 0:
                moreObjs.extend(getObjsFromConjunctions(moreObjs))
    return moreObjs


def getVerbsFromConjunctions(verbs):
    moreVerbs = []
    for verb in verbs:
        rightDeps = {tok.lower_ for tok in verb.rights}
        if "and" in rightDeps:
            moreVerbs.extend([tok for tok in verb.rights if tok.pos_ == "VERB"])
            if len(moreVerbs) > 0:
                moreVerbs.extend(getVerbsFromConjunctions(moreVerbs))
    return moreVerbs


def findSubs(tok):
    head = tok.head
    while head.pos_ != "VERB" and head.pos_ != "NOUN" and head.head != head:
        head = head.head
    if head.pos_ == "VERB":
        subs = [tok for tok in head.lefts if tok.dep_ == "SUB"]
        if len(subs) > 0:
            verbNegated = isNegated(head)
            subs.extend(getSubsFromConjunctions(subs))
            return subs, verbNegated
        elif head.head != head:
            return findSubs(head)
    elif head.pos_ == "NOUN":
        return [head], isNegated(tok)
    return [], False


def isNegated(tok):
    negations = {"no", "not", "n't", "never", "none"}
    for dep in list(tok.lefts) + list(tok.rights):
        if dep.lower_ in negations:
            return True
    return False


def findSVs(tokens):
    svs = []
    verbs = [tok for tok in tokens if tok.pos_ == "VERB"]
    for v in verbs:
        subs, verbNegated = getAllSubs(v)
        if len(subs) > 0:
            for sub in subs:
                svs.append((sub.orth_, "!" + v.orth_ if verbNegated else v.orth_))
    return svs


def getObjsFromPrepositions(deps):
    objs = []
    for dep in deps:
        if dep.pos_ == "ADP" and dep.dep_ == "prep":
            objs.extend(
                [
                    tok
                    for tok in dep.rights
                    if tok.dep_ in OBJECTS
                    or (tok.pos_ == "PRON" and tok.lower_ == "me")
                ]
            )
    return objs


def getAdjectives(toks):
    toks_with_adjectives = []
    for tok in toks:
        adjs = [left for left in tok.lefts if left.dep_ in ADJECTIVES]
        adjs.append(tok)
        adjs.extend([right for right in tok.rights if tok.dep_ in ADJECTIVES])
        tok_with_adj = " ".join([adj.lower_ for adj in adjs])
        toks_with_adjectives.extend(adjs)

    return toks_with_adjectives


def getObjsFromAttrs(deps):
    for dep in deps:
        if dep.pos_ == "NOUN" and dep.dep_ == "attr":
            verbs = [tok for tok in dep.rights if tok.pos_ == "VERB"]
            if len(verbs) > 0:
                for v in verbs:
                    rights = list(v.rights)
                    objs = [tok for tok in rights if tok.dep_ in OBJECTS]
                    objs.extend(getObjsFromPrepositions(rights))
                    if len(objs) > 0:
                        return v, objs
    return None, None


def getObjFromXComp(deps):
    for dep in deps:
        if dep.pos_ == "VERB" and dep.dep_ == "xcomp":
            v = dep
            rights = list(v.rights)
            objs = [tok for tok in rights if tok.dep_ in OBJECTS]
            objs.extend(getObjsFromPrepositions(rights))
            if len(objs) > 0:
                return v, objs
    return None, None


def getAllSubs(v):
    verbNegated = isNegated(v)
    subs = [tok for tok in v.lefts if tok.dep_ in SUBJECTS and tok.pos_ != "DET"]
    if len(subs) > 0:
        subs.extend(getSubsFromConjunctions(subs))
    else:
        foundSubs, verbNegated = findSubs(v)
        subs.extend(foundSubs)
    return subs, verbNegated


def getAllObjs(v):
    # rights is a generator
    rights = list(v.rights)
    objs = [tok for tok in rights if tok.dep_ in OBJECTS]
    objs.extend(getObjsFromPrepositions(rights))

    potentialNewVerb, potentialNewObjs = getObjFromXComp(rights)
    if (
        potentialNewVerb is not None
        and potentialNewObjs is not None
        and len(potentialNewObjs) > 0
    ):
        objs.extend(potentialNewObjs)
        v = potentialNewVerb
    if len(objs) > 0:
        objs.extend(getObjsFromConjunctions(objs))
    return v, objs


def getAllObjsWithAdjectives(v):
    # rights is a generator
    rights = list(v.rights)
    objs = [tok for tok in rights if tok.dep_ in OBJECTS]

    if len(objs) == 0:
        objs = [tok for tok in rights if tok.dep_ in ADJECTIVES]

    objs.extend(getObjsFromPrepositions(rights))

    potentialNewVerb, potentialNewObjs = getObjFromXComp(rights)
    if (
        potentialNewVerb is not None
        and potentialNewObjs is not None
        and len(potentialNewObjs) > 0
    ):
        objs.extend(potentialNewObjs)
        v = potentialNewVerb
    if len(objs) > 0:
        objs.extend(getObjsFromConjunctions(objs))
    return v, objs


def findSVOs(tokens):
    svos = []
    verbs = [tok for tok in tokens if tok.pos_ == "VERB" and tok.dep_ != "aux"]
    for v in verbs:
        subs, verbNegated = getAllSubs(v)
        # hopefully there are subs, if not, don't examine this verb any longer
        if len(subs) > 0:
            v, objs = getAllObjs(v)
            for sub in subs:
                for obj in objs:
                    objNegated = isNegated(obj)
                    svos.append(
                        (
                            sub.lower_,
                            "!" + v.lower_ if verbNegated or objNegated else v.lower_,
                            obj.lower_,
                        )
                    )
    return svos


def findSVAOs(tokens):
    svos = []
    verbs = [tok for tok in tokens if tok.pos_ == "AUX" or tok.pos_ == "VERB"] 
    #verbs = [tok for tok in tokens if tok.pos_ == "VERB" and tok.dep_ != "aux"]
    for v in verbs:
        print(v)
        subs, verbNegated = getAllSubs(v)
        # hopefully there are subs, if not, don't examine this verb any longer
        if len(subs) > 0:
            v, objs = getAllObjsWithAdjectives(v)
            for sub in subs:
                for obj in objs:
                    objNegated = isNegated(obj)
                    obj_desc_tokens = generate_left_right_adjectives(obj)
                    sub_compound = generate_sub_compound(sub)
                    svos.append(
                        (
                            " ".join(tok.lower_ for tok in sub_compound),
                            "!" + v.lower_ if verbNegated or objNegated else v.lower_,
                            " ".join(tok.lower_ for tok in obj_desc_tokens),
                        )
                    )
    return svos



codex_actions_map = {}
codex_actions_map["Find"] = ["find","get","show","list"]



codex_action_keys = list(codex_actions_map.keys())
print(codex_action_keys)
def findCodexQuery(tokens,entity_map):
    codex_query = []
    verbs = [tok for tok in tokens if tok.pos_ == "AUX" or tok.pos_ == "VERB"] 
    #verbs = [tok for tok in tokens if tok.pos_ == "VERB" and tok.dep_ != "aux"]
    for v in verbs:

        #Get the action
        for key in codex_action_keys:
            if str(v).lower() in codex_actions_map[key]:
                codex_query.append(key)


                #what is the sub for this
                curr_sub = None

                subs = [tok for tok in v.rights]
                print("sub for Find")
                print(subs)

                for sub in subs:
                    ent_keys = list(entity_map.keys())

                    for ent_key in ent_keys:

                        if str(sub).lower() in entity_map[ent_key]:
                            codex_query.append(ent_key)
                            curr_sub = sub
                            break
                    

                    if curr_sub is not None:
                        break

                break
        


        #print here we go 

        if curr_sub is not None:

            #find everything to right of curr_sub

            subs = [tok for tok in curr_sub.rights]

            print(subs)

            #check rights again?

            for sub in subs:
                curr_rights = [tok for tok in sub.rights]

                print(curr_rights)



            return codex_query
            



        
        subs, verbNegated = getAllSubs(v)
        print("this is sub")
        print(v)
        print(subs)
        print(verbNegated)
        # hopefully there are subs, if not, don't examine this verb any longer
        if len(subs) > 0:
            v, objs = getAllObjsWithAdjectives(v)
            for sub in subs:
                for obj in objs:
                    objNegated = isNegated(obj)
                    obj_desc_tokens = generate_left_right_adjectives(obj)
                    sub_compound = generate_sub_compound(sub)
                    codex_query.append(
                        (
                            " ".join(tok.lower_ for tok in sub_compound),
                            "!" + v.lower_ if verbNegated or objNegated else v.lower_,
                            " ".join(tok.lower_ for tok in obj_desc_tokens),
                        )
                    )
    return codex_query









def generate_sub_compound(sub):
    sub_compunds = []
    for tok in sub.lefts:
        if tok.dep_ in COMPOUNDS:
            sub_compunds.extend(generate_sub_compound(tok))
    sub_compunds.append(sub)
    for tok in sub.rights:
        if tok.dep_ in COMPOUNDS:
            sub_compunds.extend(generate_sub_compound(tok))
    return sub_compunds


def generate_left_right_adjectives(obj):
    obj_desc_tokens = []
    for tok in obj.lefts:
        if tok.dep_ in ADJECTIVES:
            obj_desc_tokens.extend(generate_left_right_adjectives(tok))
    obj_desc_tokens.append(obj)

    for tok in obj.rights:
        if tok.dep_ in ADJECTIVES:
            obj_desc_tokens.extend(generate_left_right_adjectives(tok))

    return obj_desc_tokens