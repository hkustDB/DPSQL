import math
import random
import sys


def ReadInput():
    global index
    global num_users
    global value_list
    global query_result
    global input_result

    id_dict = {}
    id_num = 0

    value_list = []


    for line in input_result:
        elements = list(line)

        tuple_value = float(elements[0])
        user_id = elements[1]

        if user_id in id_dict.keys():
            user_id = id_dict[user_id]
        else:
            id_dict[user_id] = id_num
            user_id = id_num
            id_num += 1

        value_list.append((user_id, tuple_value))

    num_users = len(id_dict)
    value_list = sorted(value_list, reverse=True, key=lambda item: item[1])
    if index > len(value_list):
        query_result = 0
    else:
        query_result = value_list[index - 1][1]


def ComputeValues(tau):
    global check_fs
    global num_users
    global value_list
    global index

    j = 0
    counters = {}
    check_fs = [0] * (2 * tau + 2)

    if index > len(value_list):
        return

    for i in range(num_users):
        counters[i] = 0

    num = 0
    for tuple_ in value_list:
        if num <= index - 1:
            counters[tuple_[0]] += 1
            num += 1

            continue
        else:
            counters_list = sorted(counters.values(), reverse=True)

            if sum(counters_list[j:]) <= index - 1:
                check_fs[j] = tuple_[1]
            else:
                j += 1

                if j > 2 * tau + 1:
                    return

                check_fs[j] = tuple_[1]

            counters[tuple_[0]] += 1


def RunAlgorithm():
    global check_fs
    global query_result
    global epsilon
    global beta
    global upper_bound
    global error_level
    global output_path
    global index
    global inverse_list

    tau = math.ceil(2 / epsilon * math.log((upper_bound / error_level + 1) / beta))
    ComputeValues(tau)

    inverse_list = []
    inverse_list.append(upper_bound)
    inverse_list.append(query_result)
    for j in range(1, 2 * tau + 1):
        value = check_fs[j]
        inverse_list.append(value)
    inverse_list.append(0)
    # for v in inverse_list:
    #     output_file.write(str(v) + "\n")


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


def processMaxSJA1(result, k, e=1, b=0.1, error=1, ub=200):
    global input_result
    global epsilon
    global beta
    global upper_bound
    global error_level
    global output_path
    global index

    input_result = result
    upper_bound = ub
    epsilon = e
    beta = b
    error_level = error
    index = k
    # index = 1
    # input_query = query
    # input_query = "../Test/ShiftedInverseDemo/Q9_count.txt"
    # output_path = "../Test/ShiftedInverseDemo/newQ9_test2.txt"

    # path = "../test.txt"
    # query_file = open(path, 'r')
    # for line in query_file.readlines():
    #     input_query = input_query + line
    #     if ";" in input_query:
    #         break
    ReadInput()
    RunAlgorithm()
    process_inverse()
    exponential()


def get_result():
    global query_result
    global output
    global error

    return query_result, output, error


if __name__ == "__main__":
    processMaxSJA1(sys.argv[1:], 1)
