"""
    Simple rules
"""
def nil_rule(facts):
    return None


def un_rule(facts):
    return facts


def raise_rule(ex, mes):
    def raise_warn(facts):
        m = "%s -> %s" % (mes, str(facts))
        raise ex(m)
    return raise_warn


def is_instance_rule(t):
    def is_instance(facts):
        if isinstance(facts, t):
            return True
        return False
    return is_instance


def is_null_rule(facts):
    if facts is None:
        return True
    return False


def and_rule(*bs):
    def _and(facts):
        for b in bs:
            if b(facts) is False:
                return False
        return True
    return _and


def not_rule(b):
    def _not(facts):
        if b(facts) is True:
            return False
        return True
    return _not


def decision_tree_rule(*args):
    """
        The *args must be a list[{bool, function}, {bool, function}, {bool, function}...]
        The bool can be a simple var, or you can give it an function, but the function must be return a bool
    """
    def decision(facts):
        for each in args:
            dec, action = each.items()[0]
            if isinstance(dec, bool):
                if dec is True:
                    return action(facts)
            else:
                if dec(facts) is True:
                    return action(facts)

        return False
    return decision


def sequence_rule(*rules):
    flow = rules
    def sequence(facts):
        for each in flow:
            facts = each(facts)

        return facts
    return sequence


def if_else_rule(IF, TRUE, FALSE=un_rule):
    def if_else(facts):
        if IF(facts):
            return TRUE(facts)
        else:
            return FALSE(facts)

    return if_else


def for_dict_rule(KEY_RULE=nil_rule, VAL_RULE=nil_rule):
    """
        use some rule, make dict to list
        like --> [(1, "tom"), (2, "jack"), ("at", 6)]
    :param KEY_RULE: rule for key
    :param VAL_RULE: rule for value
    :return: list
    """
    def for_dict(facts):
        result = dict()
        for k, v in facts.iteritems():
            nk = KEY_RULE(k)
            nv = VAL_RULE(v)

            if nk and nv:
                result[nk] = nv
            elif nk and not nv:
                result[nk] = None
            else:
                continue

        return result
    return for_dict


def for_list_rule(ITEM_RULE=un_rule):
    def for_list(facts):
        result = list()
        for each in facts:
            result.append(ITEM_RULE(each))
        return result
    return for_list


def func_rule(f_source, *args):
    app = eval(f_source % args)
    return app


"""
    Extend for simple rule
"""
def lower_rule(facts):
    return facts.lower()


def upper_rule(facts):
    return facts.upper()


# def replace_rule(r_aim, r_t):
#     def replace(facts):
#         return facts.replace(r_aim, r_t)
#     return replace


def join_rule(j_str):
    def join(facts):
        f = [str(a) for a in facts]
        return j_str.join(f)
    return join


def to_type_rule(t):
    def to(facts):
        return t(facts)
    return to


"rules"
NIL = nil_rule
UN_DO = un_rule
ERR = raise_rule

# bool
NOT = not_rule
AND = and_rule
IS_NULL = is_null_rule
IS = is_instance_rule

# stream
IE = if_else_rule
SEQ = sequence_rule
DECT = decision_tree_rule
FOR_DICT = for_dict_rule
FOR_LIST = for_list_rule
FUNC = func_rule

# arg transform
LOWER = lower_rule
UPPER = upper_rule
# REP = replace_rule
JOIN = join_rule
TO = to_type_rule


"""
    Assemble tool package
"""
IS_STR = IS((str, unicode))
IS_LIST = IS((list, tuple))
IS_DICT = IS(dict)
TO_STR = TO(str)
NOT_NULL = NOT(IS_NULL)


__all__ = ["NIL", "UN_DO", "ERR", "NOT", "AND", "IS_NULL",
           "IS", "IE", "SEQ", "DECT", "FOR_DICT", "FOR_LIST",
           "FUNC", "LOWER", "UPPER", "JOIN", "TO",
           "IS_STR", "IS_LIST", "IS_DICT", "TO_STR", "NOT_NULL"
           ]
