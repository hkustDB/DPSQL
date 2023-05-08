# -*- coding: utf-8 -*-
import getopt
import math
import sys
import random
import cplex
import numpy as np
import time


def LapNoise():
    a = random.uniform(0, 1)
    b = math.log(1 / (1 - a))
    c = random.uniform(0, 1)
    if c > 0.5:
        return b
    else:
        return -b


def ReadInput():
    global input_result
    # The connections between entities and join results
    global connections
    # The DS
    global downward_sensitivity
    # The aggregation values of join results
    global aggregation_values
    # The real query result
    global real_query_result
    # The dictionary to store the tuples' sensitivities
    # The number of base table tuples
    global id_num
    id_num = 0
    entities_sensitivity_dic = {}
    # The dictionary to re-id entities
    id_dic = {}
    # Collect the DS
    downward_sensitivity = 0
    connections = []
    aggregation_values = []
    # input_file = open(input_file_path, 'r')
    for line in input_result:
        elements = line
        connection = []
        # The first value is the aggregation value
        aggregation_value = float(elements[0])
        # For each entity contribution to that join result
        for element in elements[1:]:
            # element = int(element)
            # Re-order the IDs
            if element in id_dic.keys():
                element = id_dic[element]
            else:
                id_dic[element] = id_num
                element = id_num
                id_num += 1
            # Update the entity's sensitivity
            if element in entities_sensitivity_dic.keys():
                entities_sensitivity_dic[element] += aggregation_value
            else:
                entities_sensitivity_dic[element] = aggregation_value
            # Update the DS
            if downward_sensitivity <= entities_sensitivity_dic[element]:
                downward_sensitivity = entities_sensitivity_dic[element];
            connection.append(element)
        connections.append(connection)
        aggregation_values.append(aggregation_value)
    real_query_result = sum(aggregation_values)


class Optimizer(cplex.callbacks.SimplexCallback):
    def __call__(self):
        value = self.get_objective_value()
        if value < self.tar:
            self.early_stop = True
            self.abort()


def Gr(r, tar, fast_version):
    global connections
    global aggregation_values
    global downward_sensitivity
    global id_num
    if r >= downward_sensitivity:
        return id_num
    num_connections = len(connections)
    # Set the obj
    cpx = cplex.Cplex()
    cpx.objective.set_sense(cpx.objective.sense.maximize)
    # Set variables
    # For y_i's
    obj1 = np.ones(id_num)
    ub1 = np.ones(id_num)
    cpx.variables.add(obj=obj1, ub=ub1)
    # For z_j's
    obj2 = np.zeros(num_connections)
    ub2 = np.ones(num_connections)
    cpx.variables.add(obj=obj2, ub=ub2)
    # Set the right hand side and the sign
    # First, the constriant for each z_j
    rhs1 = np.zeros(num_connections)
    for j in range(num_connections):
        rhs1[j] = len(connections[j]) - 1
    senses1 = "L" * num_connections
    cpx.linear_constraints.add(rhs=rhs1, senses=senses1)
    # Second, the constriant for each entity
    rhs2 = np.ones(id_num) * r
    senses2 = "L" * id_num
    cpx.linear_constraints.add(rhs=rhs2, senses=senses2)
    # Set the coefficients
    cols = []
    rows = []
    vals = []
    # First, the constriant for each z_j
    for j in range(num_connections):
        cols.append(id_num + j)
        rows.append(j)
        vals.append(-1)
        for i in connections[j]:
            cols.append(i)
            rows.append(j)
            vals.append(1)
    # Second, the constriant for each entity
    for j in range(num_connections):
        for i in connections[j]:
            cols.append(id_num + j)
            rows.append(num_connections + i)
            vals.append(aggregation_values[j])
    cpx.linear_constraints.set_coefficients(zip(rows, cols, vals))
    cpx.set_log_stream(None)
    cpx.set_error_stream(None)
    cpx.set_warning_stream(None)
    cpx.set_results_stream(None)
    if fast_version == 1:
        optimizer = cpx.register_callback(Optimizer)
        optimizer.tar = tar
        optimizer.early_stop = False
        cpx.parameters.lpmethod.set(cpx.parameters.lpmethod.values.dual)
        cpx.solve()
        if optimizer.early_stop:
            return tar - 1
        else:
            return cpx.solution.get_objective_value()
    else:
        # Solve the LP
        cpx.parameters.lpmethod.set(cpx.parameters.lpmethod.values.dual)
        cpx.solve()
        return cpx.solution.get_objective_value()


def Truncation(r):
    global connections
    global aggregation_values
    global downward_sensitivity
    global real_query_result
    global id_num
    if r >= downward_sensitivity:
        return real_query_result
    num_constraints = id_num
    num_variables = len(connections)
    # Set the obj
    cpx = cplex.Cplex()
    cpx.objective.set_sense(cpx.objective.sense.maximize)
    # Set variables
    obj = np.ones(num_variables)
    ub = np.zeros(num_variables)
    for i in range(num_variables):
        ub[i] = aggregation_values[i]
    cpx.variables.add(obj=obj, ub=ub)
    # Set the right hand side and the sign
    rhs = np.ones(num_constraints) * r
    senses = "L" * num_constraints
    cpx.linear_constraints.add(rhs=rhs, senses=senses)
    # Set the coefficients
    cols = []
    rows = []
    vals = []
    for i in range(num_variables):
        for j in connections[i]:
            cols.append(i)
            rows.append(j)
            vals.append(1)
    cpx.linear_constraints.set_coefficients(zip(rows, cols, vals))
    cpx.set_log_stream(None)
    cpx.set_error_stream(None)
    cpx.set_warning_stream(None)
    cpx.set_results_stream(None)
    # Solve the LP
    cpx.parameters.lpmethod.set(cpx.parameters.lpmethod.values.primal)
    cpx.solve()
    return cpx.solution.get_objective_value()


def RunAlgorithm():
    global id_num
    global downward_sensitivity
    global epsilon
    global beta
    global fast_version
    T = id_num - 6 / epsilon * math.log(1 / beta) + LapNoise() * 3 / epsilon
    r = 1
    while True:
        v_i = LapNoise() * 6 / epsilon
        tar = T - v_i
        G_r = Gr(r, tar, fast_version)
        if G_r >= tar:
            break;
        r = r * 2
    res = Truncation(r) + 3 * r / epsilon * LapNoise()
    return res


def processOpt(result, e=1, b=0.1):
    # The input file including the relationships between aggregations and base tuples
    global input_result
    input_result = result
    # Privacy budget
    global epsilon
    epsilon = e
    # Error probablity: with probablity at least 1-beta, the error can be bounded
    global beta
    beta = b
    # Wehtehr to use the fast version
    global fast_version
    fast_version = 1

    # The real query result
    global real_query_result
    global noised_result
    # try:
    #     opts, args = getopt.getopt(argv, "h:I:e:b:f:", ["Input=", "epsilon=", "beta=", "fast="])
    # except getopt.GetoptError:
    #     print("OptSJA.py -I <input file> -e <epsilon(default 0.1)> -b <beta(default 0.1)> -f <fast version(default 1)>")
    #     sys.exit(2)
    # for opt, arg in opts:
    #     if opt == '-h':
    #         print(
    #             "OptSJA.py -I <input file> -e <epsilon(default 0.1)> -b <beta(default 0.1)> -f <fast version(default 1)>")
    #         sys.exit()
    #     elif opt in ("-I", "--Input"):
    #         input_file_path = str(arg)
    #     elif opt in ("-e", "--epsilon"):
    #         epsilon = float(arg)
    #     elif opt in ("-b", "--beta"):
    #         beta = float(arg)
    #     elif opt in ("-f", "--fast"):
    #         fast_version = int(arg)
    # start = time.time()
    ReadInput()
    res = RunAlgorithm()
    noised_result = res
    # end = time.time()
    # print("Query Result")
    # print(real_query_result)
    # print("Noised Result")
    # print(res)
    # print("Time")
    # print(end - start)

def get_result():
    global real_query_result
    global noised_result
    return real_query_result, noised_result

# if __name__ == "__main__":
#     main(sys.argv[1:])