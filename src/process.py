import math
from abc import abstractmethod, ABC

from pglast import ast
from pglast import parser
from pglast import stream

import src.algorithm.MultiSJA
import src.algorithm.MultiSJF
import src.algorithm.FastSJA
import src.algorithm.MaxSJA1
import src.algorithm.MaxSJA2
import src.algorithm.OptSJA
from src.parser import userAdder, ImplicitJoin, complete_query, aggregationVisit, get_primary_keys, add_table_name, \
    group_by
from src.util import pg_exec


class algorithm(ABC):
    def __init__(self, pks, fks, schema, parameters, dbsetting):
        self.pks = pks
        self.fks = fks
        self.schema = schema
        self.parameters = parameters
        self.dbsetting = dbsetting
        self.rewrite_query = None
        self.input_result = None
        self.error = None
        self.true_result = None
        self.noise_result = None

    def get_input_result(self):
        self.input_result = pg_exec(self.dbsetting, self.rewrite_query)

    @abstractmethod
    def rewrite(self, query, private_relations):
        pass

    @abstractmethod
    def process(self):
        pass


class FastSJA(algorithm):

    def rewrite(self, query, private_relations):
        private_pk = get_primary_keys(self.pks, private_relations)
        root = parser.parse_sql(query)
        selectstmt = root[0].stmt
        if not isinstance(selectstmt, ast.SelectStmt):
            raise Exception
        ImplicitJoin()(selectstmt)
        add_table_name(selectstmt, self.schema)(selectstmt)
        aggregationVisit()(selectstmt)
        userAdder(private_pk)(selectstmt)
        complete_query(self.fks)(selectstmt)
        self.rewrite_query = (stream.RawStream()(selectstmt))

    def process(self):
        epsilon = float(self.parameters['epsilon'])
        beta = float(self.parameters['beta'])
        processor_num = int(self.parameters['processor_num'])
        global_sensitivity = float(self.parameters['global_sensitivity'])
        approximate_factor = float(self.parameters['approximate_factor'])
        src.algorithm.FastSJA.processFastSJA(self.input_result, e=epsilon, b=beta, gs=global_sensitivity,
                                     p_num=processor_num, afactor=approximate_factor)
        self.true_result, self.noise_result = src.algorithm.FastSJA.get_result()


class OptSJA(FastSJA):
    def process(self):
        epsilon = float(self.parameters['epsilon'])
        beta = float(self.parameters['beta'])
        src.algorithm.OptSJA.processOpt(self.input_result, e=epsilon, b=beta)
        self.true_result, self.noise_result = src.algorithm.OptSJA.get_result()


class MultiSJF(algorithm):

    def rewrite(self, query, private_relations):
        private_pk = get_primary_keys(self.pks, private_relations)
        root = parser.parse_sql(query)
        selectstmt = root[0].stmt
        if not isinstance(selectstmt, ast.SelectStmt):
            raise Exception
        ImplicitJoin()(selectstmt)
        add_table_name(selectstmt, self.schema)(selectstmt)
        group_by()(selectstmt)
        aggregationVisit()(selectstmt)
        userAdder(private_pk)(selectstmt)
        complete_query(self.fks)(selectstmt)
        self.rewrite_query = (stream.RawStream()(selectstmt))

    def process(self):
        epsilon = float(self.parameters['epsilon'])
        beta = float(self.parameters['beta'])
        delta = float(self.parameters['delta'])
        src.algorithm.MultiSJF.ProcessMultiQSJF(self.input_result, e=epsilon, b=beta, d=delta)
        self.true_result, self.noise_result, self.error = src.algorithm.MultiSJF.get_result()


class MultiSJA(MultiSJF):

    def process(self):
        epsilon = float(self.parameters['epsilon'])
        beta = float(self.parameters['beta'])
        delta = float(self.parameters['delta'])
        src.algorithm.MultiSJA.ProcessMultiQSJA(self.input_result, e=epsilon, b=beta, Del=delta)
        self.true_result, self.noise_result, self.error = src.algorithm.MultiSJA.get_result()


class MaxSJA1(algorithm):
    def __init__(self, pks, fks, schema, parameters, dbsetting):
        super().__init__(pks, fks, schema, parameters, dbsetting)
        self.k = None

    def rewrite(self, query, private_relations):
        private_pk = get_primary_keys(self.pks, private_relations)
        root = parser.parse_sql(query)
        selectstmt = root[0].stmt
        if not isinstance(selectstmt, ast.SelectStmt):
            raise Exception
        ImplicitJoin()(selectstmt)
        add_table_name(selectstmt, self.schema)(selectstmt)
        agg = aggregationVisit()
        agg(selectstmt)
        userAdder(private_pk)(selectstmt)
        complete_query(self.fks)(selectstmt)
        self.k = agg.index
        self.rewrite_query = (stream.RawStream()(selectstmt))

    def process(self):
        if self.k == 0:
            self.k = len(self.input_result)
        epsilon = float(self.parameters['epsilon'])
        beta = float(self.parameters['beta'])
        error_level = float(self.parameters['error_level'])
        upper_bound = float(self.parameters['upper_bound'])
        src.algorithm.MaxSJA1.processMaxSJA1(self.input_result, self.k, e=epsilon, b=beta,
                                                             error=error_level, ub=upper_bound)
        self.true_result, self.noise_result, self.error = src.algorithm.MaxSJA1.get_result()


class MaxSJA2(MaxSJA1):

    def process(self):
        if self.k == 0:
            self.k = len(self.input_result)
        epsilon = float(self.parameters['epsilon'])
        beta = float(self.parameters['beta'])
        processor_num = int(self.parameters['processor_num'])
        error_level = float(self.parameters['error_level'])
        upper_bound = float(self.parameters['upper_bound'])
        src.algorithm.MaxSJA2.processMaxSJA2(self.input_result, self.k, e=epsilon, b=beta,
                                                             error=error_level, ub=upper_bound, p_num=processor_num)
        self.true_result, self.noise_result, self.error = src.algorithm.MaxSJA2.get_result()


class MultiMax(algorithm):

    def __init__(self, input_l, pks, fks, schema, parameters, dbsetting):
        super().__init__(pks, fks, schema, parameters, dbsetting)
        self.k = None
        self.input_l = input_l
        self.group_ids = []
        self.input_final_result = {}
        self.num_query = None

    def rewrite(self, query, private_relations):
        private_pk = get_primary_keys(self.pks, private_relations)
        root = parser.parse_sql(query)
        selectstmt = root[0].stmt
        if not isinstance(selectstmt, ast.SelectStmt):
            raise Exception
        ImplicitJoin()(selectstmt)
        add_table_name(selectstmt, self.schema)(selectstmt)
        group_by()(selectstmt)
        agg = aggregationVisit()
        agg(selectstmt)
        userAdder(private_pk)(selectstmt)
        complete_query(self.fks)(selectstmt)
        self.k = agg.index
        self.rewrite_query = (stream.RawStream()(selectstmt))

    def get_input_result(self):
        self.input_result = pg_exec(self.dbsetting, self.rewrite_query)
        group_id = 0
        for each_res in self.input_result:
            if each_res[0] not in self.group_ids:
                self.group_ids.append(each_res[0])
                self.input_final_result[group_id] = [each_res[1:]]
                group_id += 1
            else:
                save_idx = self.group_ids.index(each_res[0])
                self.input_final_result[save_idx].append(each_res[1:])

        self.num_query = len(self.input_final_result.keys())
        self.true_result = []
        self.noise_result = []
        self.error = 0

    def process(self):
        epsilon = float(self.parameters['epsilon'])
        beta = float(self.parameters['beta'])
        error_level = float(self.parameters['error_level'])
        upper_bound = float(self.parameters['upper_bound'])
        processor_num = int(self.parameters['processor_num'])
        delta = float(self.parameters['delta'])
        # advanced composition
        beta = beta / self.num_query
        epsilon = (math.sqrt(2 * self.num_query * math.log(1 / delta) + 4 * epsilon * self.num_query) - math.sqrt(
            2 * self.num_query * math.log(1 / delta))) / (2 * self.num_query)

        if self.input_l == 1:
            for g_id in self.input_final_result.keys():
                group = self.group_ids[g_id]
                next_input = self.input_final_result[g_id]
                if self.k == 0:
                    self.k = len(next_input)-1
                src.algorithm.MaxSJA1.processMaxSJA1(next_input, self.k, e=epsilon, b=beta,
                                                                     error=error_level, ub=upper_bound)
                true_result_k, noise_result_k, error_k = src.algorithm.MaxSJA1.get_result()
                self.true_result.append((true_result_k, group))
                self.noise_result.append((noise_result_k, group))
                self.error += error_k
        else:
            for g_id in self.input_final_result.keys():
                group = self.group_ids[g_id]
                next_input = self.input_final_result[g_id]
                if self.k == 0:
                    self.k = len(next_input)-1
                src.algorithm.MaxSJA2.processMaxSJA2(next_input, self.k, e=epsilon, b=beta,
                                                                     error=error_level, ub=upper_bound,
                                                                     p_num=processor_num)
                true_result_k, noise_result_k, error_k = src.algorithm.MaxSJA2.get_result()
                self.true_result.append((true_result_k, group))
                self.noise_result.append((noise_result_k, group))
                self.error += error_k
