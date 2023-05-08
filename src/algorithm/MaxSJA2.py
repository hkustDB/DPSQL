import random

import cplex
import math
import multiprocessing
import numpy as np


def ReadInput():
    global input_result
    global value_list
    global num_users
    global num_tuples
    global query_result

    id_dict = {}
    id_num = 0

    users = []
    tuples = []
    value_dict = {}


    for line in input_result:
        elements = list(line)

        tuple_ = []
        value = float(elements[0])

        for element in elements[1:]:
            user_id = element

            if user_id in id_dict.keys():
                user_id = id_dict[user_id]
            else:
                users.append(id_num)
                id_dict[user_id] = id_num
                user_id = id_num
                id_num += 1

            tuple_.append(user_id)

        tuple_ = tuple(sorted(set(tuple_)))

        if tuple_ in value_dict.keys():
            value_dict[tuple_].append(value)
        else:
            tuples.append(tuple_)
            value_dict[tuple_] = [value]

    num_users = len(users)

    value_list = [(users, i) for (users, values) in value_dict.items() for i in values]
    value_list = sorted(value_list, reverse=True, key=lambda item: item[1])

    num_tuples = len(value_list)
    if index > len(value_list):
        query_result = 0
    else:
        query_result = value_list[index - 1][1]


def LpSolver(num_tuples_lp):
    global value_list
    global num_users

    cpx = cplex.Cplex()
    cpx.objective.set_sense(cpx.objective.sense.minimize)

    cpx.set_log_stream(None)
    cpx.set_error_stream(None)
    cpx.set_warning_stream(None)
    cpx.set_results_stream(None)

    obj = np.append(np.ones(num_users), np.zeros(num_tuples_lp))
    ub = np.ones(num_users + num_tuples_lp)

    cols = []
    rows = []
    vals = []
    senses = "L" * (num_tuples_lp + 1)

    for i in range(num_tuples_lp):
        rows.append(i)
        cols.append(num_users + i)
        vals.append(1)

        for j in value_list[i][0]:
            rows.append(i)
            cols.append(j)
            vals.append(-1)

    for i in range(num_tuples_lp):
        rows.append(num_tuples_lp)
        cols.append(num_users + i)
        vals.append(-1)

    rhs = np.append(np.zeros(num_tuples_lp), np.array([- num_tuples_lp + index - 1]))

    cpx.variables.add(obj=obj, ub=ub)
    cpx.linear_constraints.add(rhs=rhs, senses=senses)
    cpx.linear_constraints.set_coefficients(zip(rows, cols, vals))

    cpx.solve()

    return cpx.solution.get_objective_value()


def BinarySearch(num_users_used, left_index, right_index):
    global cover_numbers

    while left_index < right_index:
        mid_index = math.floor((left_index + right_index) / 2)

        if mid_index in cover_numbers.keys():
            lp_output = cover_numbers[mid_index]
        else:
            lp_output = LpSolver(mid_index)
            cover_numbers[mid_index] = lp_output

        if lp_output >= num_users_used:
            next_right_index = mid_index
            return BinarySearch(num_users_used, left_index, next_right_index)
        else:
            next_left_index = mid_index + 1
            return BinarySearch(num_users_used, next_left_index, right_index)

    return left_index


def ThresholdRunAlgorithm(js):
    global num_tuples
    for num_users_used in js:
        num_tuples_covered = BinarySearch(num_users_used, index, num_tuples)
        check_fs[num_users_used] = value_list[num_tuples_covered - 1][1]


def RunAlgorithm():
    global query_result
    global epsilon
    global beta
    global upper_bound
    global error_level
    global output_path
    global processor_num
    global cover_numbers
    global check_fs
    global tau
    global num_tuples
    global inverse_list
    tau = math.ceil(2 / epsilon * math.log((upper_bound / error_level + 1) / beta))

    cover_numbers = multiprocessing.Manager().dict()

    check_fs = multiprocessing.Manager().dict()
    check_fs[0] = query_result

    for i in range(1, 2 * tau + 1):
        check_fs[i] = 0

    if query_result != 0:
        arrangement_of_js = []
        for i in range(processor_num):
            arrangement_of_js.append([])

        j = 0
        for i in range(1, 2 * tau + 1):
            arrangement_of_js[j].append(i)

            j = (j + 1) % processor_num

        threads = []

        for i in range(processor_num):
            threads.append(multiprocessing.Process(target=ThresholdRunAlgorithm, args=(arrangement_of_js[i],)))
            threads[i].start()

        for i in range(processor_num):
            threads[i].join()

    inverse_list = []
    inverse_list.append(upper_bound)
    inverse_list.append(query_result)
    for j in range(1, 2 * tau + 1):
        value = check_fs[j]
        inverse_list.append(value)
    inverse_list.append(0)


def process_inverse():
    global upper_bound
    global error_level
    global check_fs
    global query_result
    global inverse_list

    check_fs = []
    i = 0

    for line in inverse_list:
        if i == 1:
            query_result = float(line)

        check_fs.append(math.ceil(float(line) / error_level) * error_level)
        i += 1


def exponential():
    global check_fs
    global query_result
    global epsilon
    global beta
    global upper_bound
    global error_level
    global error
    global output

    tau = math.ceil(2 / epsilon * math.log((upper_bound / error_level + 1) / beta))

    upper_check_fs = check_fs[: tau + 2]
    mid_check_fs = check_fs[tau + 1]
    lower_check_fs = check_fs[tau + 1:]

    upper_differences = []
    lower_differences = []

    for i in range(len(upper_check_fs) - 1):
        upper_differences.append(upper_check_fs[i] - upper_check_fs[i + 1])
    for i in range(len(lower_check_fs) - 1):
        lower_differences.append(lower_check_fs[i] - lower_check_fs[i + 1])

    pdf = []
    cdf = []

    for i in range(len(upper_differences)):
        pdf.append(math.exp(epsilon / 2 * (-tau + i - 1)) * upper_differences[i])
    pdf.append(1)
    for i in range(len(lower_differences)):
        pdf.append(math.exp(epsilon / 2 * (-i - 1)) * lower_differences[i])

    for i in range(len(pdf)):
        cdf.append(sum(pdf[: i + 1]))

    error = 1e10

    sample_1 = random.uniform(0, cdf[-1])
    matches = [x for x in cdf if x <= sample_1]
    index = len(matches)

    if index <= tau:
        sample_2 = math.floor(random.random() * upper_differences[index] / error_level) * error_level
        output = upper_check_fs[index] - sample_2
        error = abs(upper_check_fs[index] - sample_2 - query_result)
    elif index == tau + 1:
        output = mid_check_fs
        error = abs(mid_check_fs - query_result)
    elif index > tau + 1:
        sample_2 = math.floor(random.random() * lower_differences[index - tau - 2] / error_level) * error_level
        output = lower_check_fs[index - tau - 2] - sample_2 - error_level
        error = abs(lower_check_fs[index - tau - 2] - sample_2 - error_level - query_result)


def processMaxSJA2(result, k, e=1, b=0.1, error=0.1, ub=100000, p_num=1):
    global input_result
    global epsilon
    global beta
    global upper_bound
    global error_level
    global output_path
    global index
    global processor_num

    # default on unix is fork, and default on macOS is spawn
    # multiprocessing.set_start_method("fork")
    input_result = result
    # output_path = "../Test/ShiftedInverseDemo/Q7_inverse_2.txt"
    upper_bound = ub
    epsilon = e
    beta = b
    error_level = error
    index = k
    # index = 1
    # path = "../test.txt"
    # query_file = open(path, 'r')
    # for line in query_file.readlines():
    #     input_query = input_query + line
    #     if ";" in input_query:
    #         break
    processor_num = p_num

    ReadInput()
    RunAlgorithm()
    process_inverse()
    exponential()

    # print(str(error) + " " + str(output) + " " + str(query_result))


def get_result():
    global query_result
    global output
    global error
    return query_result, output, error


if __name__ == "__main__":
    processMaxSJA2()
