from pathlib import Path

import mosek
import math
import sys
import random
import numpy as np


def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent


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
    # The dimension of query: integer
    global d
    # Query size: interger
    global N
    # Join result size: d dimensional vector
    global M
    # The total number of join results
    global total_M
    # The join result set for each user: Cik represents the list of join results corresponding to user i in dimension k
    # Each Ci is a dic since each user only corresponds to a few of dimensions
    global C
    # The user set for each join result: Dkj represents the list of users corresponding to join result j in dimension k
    global D
    # The function value for each join result
    global values
    # Downward sensitivity
    global DS
    # Record which dimensions each user contributes to, each element is also a list
    global user_contribution_dim
    # The total size of above
    global total_contribution_dim
    # The sum of M_k' for all k' < k
    global M_prefix
    # Real query result
    global Q
    # Initialize the data
    global group
    group = []
    N = 0
    M = []
    total_M = 0
    C = []
    D = []
    values = []
    DS = 0
    user_contribution_dim = []
    total_contribution_dim = 0
    M_prefix = []
    # Use to re-assign new IDs for users
    user_dic = {}
    # Enumerate all input files, each one represent one query
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

    d = len(input_final_result.keys())
    Q = np.zeros(d)
    for k in range(d):
        D.append([])
        values.append([])
    k = 0
    M_prefix.append(0)
    for g_id in input_final_result.keys():
        # For each query
        #input_file = open(input_file_name, 'r')
        group_input = input_final_result[g_id]
        # Record the join result size for each query
        Mk = 0
        # Read each line
        for line in group_input:
            elements = list(line)
            # The first column is the funcation value
            values[k].append(float(elements[0]))
            Q[k] += float(elements[0])
            # Initialize the user set for join result j in dimension k
            Dkj = []
            for user in elements[1:]:
                # Re-assign ID for user
                if user not in user_dic:
                    user_dic[user] = N
                    user = N
                    N += 1
                else:
                    user = user_dic[user]
                Dkj.append(user)
            D[k].append(Dkj)
            Mk += 1
        total_M += Mk
        M_prefix.append(total_M)
        M.append(Mk)
        group.append(group_ids[g_id])
        k += 1
    for i in range(N):
        C.append({})
        user_contribution_dim.append([])
    # Set C based on D
    for k in range(d):
        for j in range(M[k]):
            for i in D[k][j]:
                if k not in user_contribution_dim[i]:
                    user_contribution_dim[i].append(k)
                    total_contribution_dim += 1
                    C[i][k] = []
                C[i][k].append(j)

    # Compute DS
    for i in range(N):
        DSi = 0
        for k in user_contribution_dim[i]:
            DSik = 0
            for j in C[i][k]:
                DSik += values[k][j]
            DSi = DSi + DSik * DSik
        if DS < DSi:
            DS = DSi
    DS = np.sqrt(DS)


# Define a stream printer to grab output from MOSEK
def streamprinter(text):
    sys.stdout.write(text)
    sys.stdout.flush()


def ECP(r):
    # Variables N number of y_i's, M number of z_j's, total_contribution_dim number of w_ik's, and N number of constant r
    num_variables = N + total_M + total_contribution_dim + N
    with mosek.Env() as env:
        # Attach a printer to the environment
        env.set_Stream(mosek.streamtype.log, streamprinter)
        env.putlicensepath(str(get_project_root()) + "/Profile/mosek.lic")
        with env.Task(0, 0) as task:
            # Attach a printer to the task
            # task.set_Stream(mosek.streamtype.log, streamprinter)
            # First, deal with variables
            # The type pf constraint for each variable
            bkx = [mosek.boundkey.ra] * N + [mosek.boundkey.lo] * total_M + [
                mosek.boundkey.lo] * total_contribution_dim + [mosek.boundkey.fx] * N
            # The lower bound for each variable
            blx = [0.0] * (N + total_M + total_contribution_dim) + [r] * N
            # The upper bound for each variable
            bux = [1.0] * (N + total_M + total_contribution_dim) + [r] * N
            # The factors for obj
            c = [1.0] * N + [0.0] * (total_M + total_contribution_dim + N)
            # Set the variables
            task.appendvars(num_variables)
            for i in range(num_variables):
                # Set the factors for the objective function
                task.putcj(i, c[i])
                # Set the constraints for the variables
                task.putvarbound(i, bkx[i], blx[i], bux[i])

            # Then, deal with the linear constraints
            task.appendcons(total_M + total_contribution_dim)
            # The first ones are for total_M number of z_j's
            for k in range(d):
                M_prefix_k = M_prefix[k]
                for j in range(M[k]):
                    # asub is for the IDs of variables
                    asub = []
                    # aval is for the factors of variables
                    aval = []
                    for i in D[k][j]:
                        asub.append(i)
                        aval.append(1.0)
                    asub.append(N + M_prefix_k + j)
                    aval.append(-1.0)
                    task.putarow(M_prefix_k + j, asub, aval)
                    task.putconbound(M_prefix_k + j, mosek.boundkey.up, len(D[k][j]) - 1.0, len(D[k][j]) - 1.0)
            # The others are for w's
            w_id = 0
            for i in range(N):
                for k in user_contribution_dim[i]:
                    asub = []
                    aval = []
                    M_prefix_k = M_prefix[k]
                    for j in C[i][k]:
                        asub.append(N + M_prefix_k + j)
                        aval.append(values[k][j])
                    asub.append(N + total_M + w_id)
                    aval.append(-1.0)
                    task.putarow(total_M + w_id, asub, aval)
                    task.putconbound(total_M + w_id, mosek.boundkey.fx, 0, 0)
                    w_id += 1

            # Finally is for cone constraints
            w_id = 0
            for i in range(N):
                submem = []
                submem.append(N + total_M + total_contribution_dim + i)
                for k in range(len(user_contribution_dim[i])):
                    submem.append(N + total_M + w_id)
                    w_id += 1
                task.appendcone(mosek.conetype.quad, 0.0, submem)
            task.putobjsense(mosek.objsense.maximize)
            task.optimize()

            # Extract the solutions
            task.solutionsummary(mosek.streamtype.msg)
            xx = [0.] * num_variables
            task.getxx(mosek.soltype.itr, xx)
            obj = np.sum(xx[0:N])
            return obj, xx


def ComputeTruncatedQuery():
    global Q_I_r
    Q_I_r = np.zeros(d)
    for k in range(d):
        for j in range(M[k]):
            temp = -len(D[k][j]) + 1
            for i in D[k][j]:
                temp += I_star[i]
            temp = max(0, temp)
            Q_I_r[k] += values[k][j] * temp


def RunAlgorithm():
    global I_star
    global Q_tilde
    base = 2
    T = - 20 / epsilon * math.log(1 / beta) + N
    T_tilde = T + LapNoise() * 20 / epsilon
    # The E's for all r's
    E = {}
    I_sol = {}
    u = {}
    r = base
    # Pre-compute the noise
    while r < DS:
        u[r] = LapNoise() * 40 / epsilon
        r *= base
    r /= base
    pre_E = N
    # Compute necessary E's
    while r >= base:
        if pre_E + u[r] < T_tilde:
            E[r] = T_tilde - u[r] - 1
        else:
            E[r], I_sol[r] = ECP(r)
            pre_E = E[r]
        r /= base
    # Conpute tilde r
    r = base
    while (True):
        if r >= DS:
            E[r] = N
            I_sol[r] = np.ones(N)
            u[r] = LapNoise() * 40 / epsilon
        if E[r] + u[r] >= T_tilde:
            I_star = I_sol[r]
            E_r = E[r]
            break
        r *= base

    temp = delta / (2 * pow(math.e, 0.55 * epsilon))
    T_hat = 2 * (N - E_r) + LapNoise() * 2 / (0.45 * epsilon) + 2 / (0.45 * epsilon) * math.log(
        pow(math.e, epsilon * 0.55) / delta)
    Gauss_noise = T_hat * r * math.sqrt(2 * math.log(1 / temp)) * (1 + 0.45 * epsilon / (4 * math.log(1 / temp))) / (
                0.45 * epsilon) * np.random.normal(0, 1, d)
    ComputeTruncatedQuery()
    Q_tilde = Q_I_r + Gauss_noise


def ProcessMultiQSJA(input, e=4, b=0.1, Del=0.000001):
    # The input file including the relationships between aggregations and base tuples
    global input_result
    input_result = input
    # Privacy budget
    global epsilon
    epsilon = e
    # Error probablity: with probablity at least 1-beta, the error can be bounded
    global beta
    global delta
    beta = b
    delta = Del
    global real_query_result
    global noised_result
    global total_error

    ReadInput()

    RunAlgorithm()
    real_query_result = Q
    noised_result = Q_tilde
    Q_diff = Q - Q_tilde
    error = 0
    error_rate = 0.0
    errors = []
    error_rates = []
    for k in range(d):
        errors.append(Q_diff[k])
        error_rates.append(abs(Q_diff[k] / Q[k]))
        error += Q_diff[k] * Q_diff[k]
        error_rate += abs(Q_diff[k] / Q[k])
    error = np.sqrt(error)

    total_error = error

    noised_result = list(zip([q for q in noised_result], [g for g in group]))
    real_query_result = list(zip([q for q in Q], [g for g in group]))


def get_result():
    global real_query_result
    global noised_result
    global total_error
    return real_query_result, noised_result, total_error


if __name__ == '__main__':
    pass

