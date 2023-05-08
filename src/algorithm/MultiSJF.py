import math
import random
import numpy as np


def ReadInput():
    global items
    global result
    global num_query
    global final_s
    global S
    global input_result
    global group

    group_ids = []
    input_final_result = {}
    group_id = 0
    for each_res in input_result:
        if each_res[0] not in group_ids:
            group_ids.append(each_res[0])
            input_final_result[group_id] = [each_res[1:]]
            group_id += 1
        else:
            save_idx = group_ids.index(each_res[0])
            input_final_result[save_idx].append(each_res[1:])

    num_query = len(input_final_result.keys())
    items = []
    dic = {}
    result = []
    values = [[] for i in range(num_query)]
    connections = [[] for i in range(num_query)]
    group = []

    idx = 0
    i = 0
    for g_id in input_final_result.keys():
        # For each query
        group_input = input_final_result[g_id]

        for line in group_input:
            elements = list(line)
            values[i].append(float(elements[0]))

            if elements[1] not in dic:
                dic[elements[1]] = idx
                elements[1] = dic[elements[1]]
                items.append(elements[1])
                idx += 1
            else:
                elements[1] = dic[elements[1]]

            connections[i].append(elements[1])

        result.append(sum(values[i]))
        group.append(group_ids[g_id])
        i += 1

    N = len(items)
    S = np.zeros((N, num_query))

    for k in range(num_query):
        for j in range(len(connections[k])):
            idx = connections[k][j]
            S[idx, k] += values[k][j]

    final_s = []
    for i in range(N):
        final_s.append(math.sqrt(sum(S[i, :] ** 2)))


def LapNoise():
    a = random.uniform(0, 1)
    b = math.log(1 / (1 - a))
    c = random.uniform(0, 1)
    if c > 0.5:
        return b
    else:
        return -b


def calculate_E(threshold):
    r = threshold
    count = 0
    N = len(items)
    Q = np.zeros(num_query)
    for i in range(N):
        if final_s[i] > r:
            Q += r * S[i] / final_s[i]
        else:
            Q += S[i]
            count += 1

    return count, Q


def RunAlgorithm():
    global epsilon
    global beta
    global delta

    N = len(items)
    T = - 60 / epsilon * math.log(4 / beta)
    T_hat = T + LapNoise() * 20 / (1 * epsilon)
    base = 1.3
    i = 0
    while (True):
        noise = LapNoise() * 40 / epsilon
        E, Q = calculate_E(pow(base, i))
        F = E - N
        F_hat = F + noise
        if F_hat > T_hat:
            tau = pow(base, i)
            break
        i += 1

    noises = tau * math.sqrt(2 * math.log(1 / delta)) * (1 + 0.9 * epsilon / (4 * math.log(1 / delta))) / (
                0.9 * epsilon) * np.random.normal(0, 1, num_query)
    Q = Q + noises

    return Q


def ProcessMultiQSJF(input, e=1, b=0.2, d=0.0000001):
    # The input file including the relationships between aggregations and base tuples
    global input_result
    input_result = input
    # Privacy budget
    global epsilon
    epsilon = e
    global delta
    delta = d
    # Error probablity: with probablity at least 1-beta, the error can be bounded
    global beta
    beta = b
    global num_query
    global real_query_result
    global noised_result
    global total_error
    global group
    # query_file = open("../test.txt", 'r')
    # for line in query_file.readlines():
    #     input_query = input_query + line
    #     if ";" in input_query:
    #         break
    ReadInput()
    Q = RunAlgorithm()
    real_query_result = result
    noised_result = Q
    abs_error = []
    error_rate = []
    total_error = 0.0
    for i in range(num_query):
        total_error += abs(Q[i] - result[i]) ** 2
        abs_error.append(abs(Q[i] - result[i]))
        error_rate.append(abs(Q[i] - result[i]) / result[i])

    total_error = np.sqrt(total_error)

    # add group to the result
    noised_result = list(zip([q for q in Q], [g for g in group]))
    real_query_result = list(zip([q for q in real_query_result], [g for g in group]))

def get_result():
    global real_query_result
    global noised_result
    global total_error
    return real_query_result, noised_result, total_error


if __name__ == '__main__':
    pass
