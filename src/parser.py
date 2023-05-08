from src.util import get_table_attribute
from pglast import ast
from pglast import visitors
from pglast import enums
from collections import deque
from pglast.visitors import Ancestor


class ImplicitJoin(visitors.Visitor):

    def __init__(self):
        self.right = []
        self.left = []
        self.qual = []

    def iterate(self, node):
        """
        we modify one thing different from default BFS:
            the select statement will be the last node to be visited
        everything else is the same as BFS, also the same as the default official implementation
        """
        select = None

        pending_updates = []

        todo = deque()

        if isinstance(node, (tuple, ast.Node)):
            todo.append((Ancestor(), node))
        else:
            raise ValueError('Bad argument, expected a ast.Node instance or a tuple')

        while todo:
            ancestors, node = todo.popleft()

            # Here `node` may be either one AST node, a tuple of AST nodes (e.g.
            # SelectStmt.targetList), or even a tuple of tuples of AST nodes (e.g.
            # SelectStmt.valuesList). To simplify code, coerce it to a sequence.

            is_sequence = isinstance(node, tuple)
            if is_sequence:
                nodes = list(node)
            else:
                nodes = [node]

            index = 0
            while nodes:
                sub_node = nodes.pop(0)
                if is_sequence:
                    sub_ancestors = ancestors / (node, index)
                else:
                    sub_ancestors = ancestors
                if isinstance(sub_node, ast.Node):
                    # pass select statement
                    if isinstance(sub_node, ast.SelectStmt):
                        # if there is a group by, we dont process it
                        # this may be revisited in the later prase
                        if sub_node.groupClause is not None:
                            return StopIteration
                        action = visitors.Continue
                        select = sub_node
                    else:
                        action = yield sub_ancestors, sub_node
                    if action is visitors.Continue:
                        for member in sub_node:
                            value = getattr(sub_node, member)
                            if isinstance(value, (tuple, ast.Node)):
                                todo.append((sub_ancestors / (sub_node, member), value))
                    elif action is visitors.Skip:
                        pass
                    else:
                        pending_updates.append(sub_ancestors.update(action))
                elif isinstance(sub_node, tuple):
                    for sub_index, value in enumerate(sub_node):
                        if isinstance(value, (tuple, ast.Node)):
                            todo.append((sub_ancestors / (sub_node, sub_index), value))

                index += 1

        for pending_update in pending_updates:
            pending_update.apply()
            if pending_update.member is None:
                self.root = pending_update.node

        # generate select statement at the end
        yield Ancestor, select

    visit = None
    """
    The default *visit* method for any node without a specific one.
    When ``None``, nothing happens.
    """

    def visit_JoinExpr(self, ancestors, node):
        """
            we keep all the table name including renaming, and all the predicate
            in the join condition
        """
        idx = 0
        # left is table name
        if isinstance(node.larg, ast.RangeVar):
            self.left.append(node.larg)
        # multiple join
        elif isinstance(node.larg, ast.JoinExpr):
            sub_ancestors = ancestors / (node.larg, idx)
            self.visit_JoinExpr(sub_ancestors, node.larg)
            idx += 1
        if isinstance(node.rarg, ast.RangeVar):
            self.right.append(node.rarg)
        # multiple join
        elif isinstance(node.rarg, ast.JoinExpr):
            sub_ancestors = ancestors / (node.rarg, idx)
            self.visit_JoinExpr(sub_ancestors, node.rarg)
            idx += 1
        # append join condition
        self.qual.append(node.quals)
        # delete the visited join node
        # we only consider inner join now (because parser will parse into this type)
        if node.jointype == enums.JoinType.JOIN_INNER:
            return visitors.Delete
        else:
            raise Exception

    def visit_SelectStmt(self, ancestors, node):
        """
        we add all the information we needed to the corresponding place in the select statement
        """
        # selected table name
        if node.fromClause is None:
            node.fromClause = ()
        for item in self.left:
            node.fromClause += (item,)
        for item in self.right:
            node.fromClause += (item,)
        # join condition
        if node.whereClause is None:
            if len(self.qual) == 1:
                node.whereClause = self.qual[0]
            elif len(self.qual) > 1:
                node.whereClause = ast.BoolExpr(boolop=enums.BoolExprType.AND_EXPR, args=())
                for item in self.qual:
                    node.whereClause.args += (item,)
        else:
            temp = node.whereClause
            node.whereClause = ast.BoolExpr(boolop=enums.BoolExprType.AND_EXPR, args=(temp,))
            for item in self.qual:
                node.whereClause.args += (item,)


class aggregationVisit(visitors.Visitor):

    def __init__(self):
        self.root = None
        self.index = None

    def visit_ResTarget(self, ancestors, node):
        """
        simply remove the written aggregation functions
        """
        if ancestors[1] is self.root:
            # we don't need aggregation function now
            # all the written code does not have aggregation
            # may be revisited for max later
            if isinstance(node.val, ast.FuncCall):
                if node.val.funcname[0].val == 'sum' or node.val.funcname[0].val == 'count' or node.val.funcname[0].val == 'max':
                    return visitors.Delete

    def visit_SelectStmt(self, ancestors, node):
        """
        rewrite count() to 1
        rewrite sum(A*B) to A*B
        """
        self.root = node
        for item in node.targetList:
            if isinstance(item.val, ast.FuncCall):
                if len(item.val.funcname) == 1:
                    if item.val.funcname[0].val == 'count':
                        node.targetList += (ast.ResTarget(val=ast.A_Const(val=ast.Integer(1))),)
                    elif item.val.funcname[0].val == 'sum':
                        temp = item.val.args[0]
                        node.targetList += (ast.ResTarget(val=temp),)
                    elif item.val.funcname[0].val == 'max':
                        temp = item.val.args[0]
                        self.index = item.val.args[1].val.val
                        node.targetList += (ast.ResTarget(val=temp),)
                else:
                    pass


def get_primary_keys(pks, relations):
    """
    :param pks: all the primary keys in the schema
    :param relations: user input private relations
    :return: private relation with their primary keys
    """
    relation = [r.strip() for r in relations.split(",")]
    res = []
    for pk in pks:
        if pk[0] in relation:
            left = pk[2].find("(")
            right = pk[2].find(")")
            key = pk[2][left + 1:right]
            res.append(pk[0] + "." + key)
    return res


class userAdder(visitors.Visitor):

    def __init__(self, keys):
        # input are private relations
        self.keys = keys

    def visit_SelectStmt(self, ancestors, node):
        """
        add the primary key of the private relation to the select statement
        """
        idx = 0
        # get all the renaming
        # not consider self join case for private relation for now
        # (i.e. there is only one renaming in the select statement for private relation)
        renaming = get_rename()
        renaming(node)
        renaming = renaming.rename_dict
        # add all the private keys into the select statement
        for r in self.keys:
            table_attribute = r.split(".")
            for name in renaming[table_attribute[0]]:
                rename = 'id' + str(idx)
                table = ast.String(value=rename)
                attri = ast.ColumnRef(fields=(name, table_attribute[1]))
                node.targetList += (ast.ResTarget(val=ast.FuncCall(funcname=(ast.String(value='concat'),),
                                                                   args=(table, attri)), name=rename),)
                # node.targetList += (ast.ResTarget(val=ast.ColumnRef(fields=(name,
                #                                                             table_attribute[1])), name=rename),)
                idx += 1


class check_expr(visitors.Visitor):
    """
    this class is simply check whether there is an expression which is the same as the input
    """

    def __init__(self, predicate):
        # input is the predicate that we want to check
        self.pred = predicate
        self.found = False
        self.test = False

    def visit_A_Expr(self, ancestors, node):
        self.found = self.found or (node == self.pred)


class complete_query(visitors.Visitor):
    """
    this visitor will make the input query complete
    i.e. find all the primary keys which are pointed to in the query by the foreign key

    Note: some part of this function is redundant, and need to optimize later
           we may focus on the optimization later.
    """

    def __init__(self, fks):
        self.fk_dic = {}
        for fk in fks:
            src_table = fk[0]
            split = fk[2].split("REFERENCES ")
            left1 = split[0].find("(")
            right1 = split[0].find(")")
            src_key = split[0][left1 + 1:right1]
            left2 = split[1].find("(")
            right2 = split[1].find(")")
            dest_key = split[1][left2 + 1:right2]
            dest_table = split[1][0:left2]
            self.fk_dic[(src_table, src_key)] = (dest_table, dest_key)

    def check_condition(self, node, src_rename, dest_rename, src_key, dest_key):
        Break = False
        # for each dest table, there are four cases for foreign key condition
        for dest in dest_rename:
            # r1.k1 = r2.k2
            c1 = ast.A_Expr(kind=enums.A_Expr_Kind.AEXPR_OP, name=(ast.String(value="="),),
                            lexpr=ast.ColumnRef(
                                fields=(ast.String(value=src_rename), ast.String(value=src_key[0].strip()))),
                            rexpr=ast.ColumnRef(
                                fields=(ast.String(value=dest), ast.String(value=dest_key[0].strip()))))

            # r2.k2 = r1.k1
            c2 = ast.A_Expr(kind=enums.A_Expr_Kind.AEXPR_OP, name=(ast.String(value="="),),
                            lexpr=ast.ColumnRef(
                                fields=(ast.String(value=dest), ast.String(value=dest_key[0].strip()),)),
                            rexpr=ast.ColumnRef(
                                fields=(ast.String(value=src_rename), ast.String(value=src_key[0].strip()),)))

            # k1 = k2
            c3 = ast.A_Expr(kind=enums.A_Expr_Kind.AEXPR_OP, name=(ast.String(value="="),),
                            lexpr=ast.ColumnRef(
                                fields=(ast.String(value=src_key[0].strip()),)),
                            rexpr=ast.ColumnRef(
                                fields=(ast.String(value=dest_key[0].strip()),)))

            # k2 = k1
            c4 = ast.A_Expr(kind=enums.A_Expr_Kind.AEXPR_OP, name=(ast.String(value="="),),
                            lexpr=ast.ColumnRef(
                                fields=(ast.String(value=dest_key[0].strip()),)),
                            rexpr=ast.ColumnRef(
                                fields=(ast.String(value=src_key[0].strip()),)))

            r1 = check_expr(c1)
            r1(node.whereClause)
            r2 = check_expr(c2)
            r2(node.whereClause)
            r3 = check_expr(c3)
            r3(node.whereClause)
            r4 = check_expr(c4)
            r4(node.whereClause)
            result = r1.found or r2.found or r3.found or r4.found
            # if we found there is a valid condition, we break the loop
            # and return the result
            if result:
                Break = True
                break
        return Break

    def visit_SelectStmt(self, ancestors, node):
        # relations = list(visitors.referenced_relations(node))
        # keep all the current renaming
        renaming = get_rename()
        renaming(node)
        relation_dict = renaming.rename_dict
        flag = True
        while flag:
            append_list = []
            visited = False
            for fk in self.fk_dic.keys():
                # if src table is in the current query, and destination table is not in
                # we add the destination table into the query, and the join condition
                if fk[0] in relation_dict.keys() and self.fk_dic[fk][0] not in relation_dict.keys():
                    # renaming the upcoming table
                    rename = self.fk_dic[fk][0] + str(0)
                    # syntax node for this table
                    item = ast.RangeVar(relname=self.fk_dic[fk][0], inh=True, alias=ast.Alias(aliasname=rename))
                    # add to the select statement
                    node.fromClause += (item,)
                    # update renaming
                    relation_dict[self.fk_dic[fk][0]] = [rename]
                    # initialize the syntax node for join condition
                    conditions = ()
                    src_key = fk[1].split(",")
                    dest_key = self.fk_dic[fk][1].split(",")
                    for i in range(len(src_key)):
                        c = ast.A_Expr(kind=enums.A_Expr_Kind.AEXPR_OP, name=(ast.String(value="="),),
                                       lexpr=ast.ColumnRef(fields=(ast.String(value=relation_dict[fk[0]][0]),
                                                                   ast.String(value=src_key[i].strip()))),
                                       rexpr=ast.ColumnRef(
                                           fields=(ast.String(value=rename), ast.String(value=dest_key[i].strip()))))
                        # print(stream.RawStream()(c))
                        conditions += (c,)
                    # add the join condition to the select statement
                    if node.whereClause is None:
                        if len(conditions) == 1:
                            node.whereClause = conditions
                        elif len(conditions) > 1:
                            node.whereClause = ast.BoolExpr(boolop=enums.BoolExprType.AND_EXPR, args=())
                            for condition in conditions:
                                node.whereClause.args += (condition,)
                    else:
                        temp = node.whereClause
                        node.whereClause = ast.BoolExpr(boolop=enums.BoolExprType.AND_EXPR, args=(temp,))
                        for condition in conditions:
                            node.whereClause.args += (condition,)
                    visited = True
                # if both src and dest tables are in the query
                # we check if the foreign key condition is in the whereClause
                if fk[0] in relation_dict.keys() and self.fk_dic[fk][0] in relation_dict.keys():
                    src_rename = relation_dict[fk[0]]
                    dest_rename = relation_dict[self.fk_dic[fk][0]]
                    src_key = fk[1].split(",")
                    dest_key = self.fk_dic[fk][1].split(",")
                    # for each renaming of the src table
                    # we check there is at least one dest table renaming connecting to this src table
                    for src in src_rename:
                        if not (self.check_condition(node, src, dest_rename, src_key, dest_key)):
                            # add another destination table renaming
                            rename = self.fk_dic[fk][0] + str(len(relation_dict[self.fk_dic[fk][0]]))
                            relation_dict[self.fk_dic[fk][0]].append(rename)
                            item = ast.RangeVar(relname=self.fk_dic[fk][0], inh=True, alias=ast.Alias(aliasname=rename))
                            # add to the select statement
                            node.fromClause += (item,)
                            conditions = ()
                            for i in range(len(src_key)):
                                c = ast.A_Expr(kind=enums.A_Expr_Kind.AEXPR_OP, name=(ast.String(value="="),),
                                               lexpr=ast.ColumnRef(
                                                   fields=(ast.String(value=src),
                                                           ast.String(value=src_key[i].strip()))),
                                               rexpr=ast.ColumnRef(
                                                   fields=(ast.String(value=rename),
                                                           ast.String(value=dest_key[i].strip()))))
                                conditions += (c,)
                            # add the join condition to the select statement
                            if node.whereClause is None:
                                if len(conditions) == 1:
                                    node.whereClause = conditions
                                elif len(conditions) > 1:
                                    node.whereClause = ast.BoolExpr(boolop=enums.BoolExprType.AND_EXPR, args=())
                                    for condition in conditions:
                                        node.whereClause.args += (condition,)
                            else:
                                temp = node.whereClause
                                node.whereClause = ast.BoolExpr(boolop=enums.BoolExprType.AND_EXPR, args=(temp,))
                                for condition in conditions:
                                    node.whereClause.args += (condition,)

            flag = visited


class add_table_name(visitors.Visitor):
    '''
    add the table either renaming or not to the column
    '''
    def __init__(self, select_node, schema):
        # get the renaming of the current node
        renaming = get_rename()
        renaming(select_node)
        self.rename_dict = renaming.rename_dict
        self.schema = schema

    def visit_ColumnRef(self, ancestors, node):
        # get fields
        fields = node.fields
        if len(fields) == 1:
            attribute = fields[0].val
            for table in self.rename_dict.keys():
                table_attr = self.schema[table]
                if attribute in table_attr:
                    # self.rename_dict[table] length should be 1, otherwise this
                    # is an ambiguous column
                    node.fields = (ast.String(value=self.rename_dict[table][0]),) + node.fields


class get_rename(visitors.Visitor):
    '''
    get all the current renaming in tis select statement
    this is mainly a helper visitor for other functional visitor
    '''

    def __init__(self):
        self.rename_dict = {}

    def visit_SelectStmt(self, ancestors, node):
        for item in node.fromClause:
            if isinstance(item, ast.RangeVar):
                if item.relname not in self.rename_dict.keys():
                    if item.alias is None:
                        self.rename_dict[item.relname] = [item.relname]
                    else:
                        self.rename_dict[item.relname] = [item.alias.aliasname]
                else:
                    if item.alias is None:
                        self.rename_dict[item.relname].append(item.relname)
                    else:
                        self.rename_dict[item.relname].append(item.alias.aliasname)


class group_by(visitors.Visitor):

    def __init__(self):
        self.root = None
        self.group = None

    def visit_ResTarget(self, ancestors, node):
        """
        simply remove the written aggregation functions
        """
        if node.val in self.group:
            return visitors.Delete

    def visit_SelectStmt(self, ancestors, node):
        # remove the group column to the selection (targetList)
        self.root = node
        self.group = node.groupClause
        new_selection = ast.FuncCall(funcname=(ast.String(value='concat'),), args=self.group)
        node.targetList = (ast.ResTarget(val=new_selection),) + node.targetList
        node.groupClause = None


class get_subquery(visitors.Visitor):

    def __init__(self):
        self.node = None

    def visit_SelectStmt(self, ancestors, node):
        if isinstance(node.fromClause[0], ast.RangeSubselect):
            self.node = node.fromClause[0].subquery


class check_type(visitors.Visitor):
    """
    this visitor will check the type of the input query
    and then decide which algorithm to process the input query
    """
    def __init__(self, relations):
        self.subquery = False
        self.groupby = False
        self.selfjoin = False
        self.max = None
        self.l = 0
        self._relation = relations

    def visit_SelectStmt(self, ancestors, node):
        if isinstance(node.fromClause[0], ast.RangeSubselect):
            self.subquery = True
            for item in node.targetList:
                if isinstance(item.val, ast.FuncCall):
                    if len(item.val.funcname) == 1:
                        if item.val.funcname[0].val == 'max':
                            self.max = True
        else:
            # if there is a subquery, we have to get implicit join first
            if self.subquery:
                ImplicitJoin()(node)
            # check max and min
            for item in node.targetList:
                if isinstance(item.val, ast.FuncCall):
                    if len(item.val.funcname) == 1:
                        if item.val.funcname[0].val == 'max':
                            self.max = True
            # check l
            renaming = get_rename()
            renaming(node)
            relation_dict = renaming.rename_dict
            for k in relation_dict.keys():
                # check for l
                if k in self._relation:
                    self.l += len(relation_dict[k])
                # check for selfjoin
                if len(relation_dict[k]) >= 2:
                    self.selfjoin = True

            # check group by
            if node.groupClause is not None:
                self.groupby = True


if __name__ == '__main__':
    pass