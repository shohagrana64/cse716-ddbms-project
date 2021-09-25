import Db_util2
# RSM= Relation Site Matrix; Which site has access to which relations/tables
RSM = [[1, 1, 1, 1, 1, 0, 0, 0],
       [1, 1, 1, 1, 1, 0, 0, 0],
       [1, 0, 0, 0, 0, 1, 0, 1],
       [0, 0, 0, 0, 0, 1, 0, 0],
       [1, 1, 1, 0, 0, 1, 1, 0],
       [1, 0, 0, 0, 0, 1, 1, 0],
       [0, 1, 1, 1, 1, 1, 0, 0],
       [1, 1, 1, 1, 0, 0, 1, 1],
       [0, 0, 0, 0, 0, 0, 0, 1],
       [1, 0, 1, 0, 0, 0, 0, 0],
       [0, 1, 1, 1, 0, 0, 1, 0],
       [0, 1, 0, 0, 0, 0, 0, 1],
       [0, 0, 1, 0, 0, 0, 0, 0],
       [0, 0, 0, 1, 0, 0, 0, 1],
       [1, 0, 1, 1, 1, 0, 0, 0],
       [0, 1, 1, 0, 1, 1, 1, 0]]

# QEP(Query Execution Plan): 20 different queries. each query requires which site for which relation/table
queryPlan = {1: [1, 1, 2, 2, 2, 3, 5, 3],
             2: [3, 5, 7, 15, 4, 6, 8],
             3: [6, 7, 8, 11, 16, 6, 8, 9],
             4: [10, 11, 11, 15, 16, 11, 16, 14],
             5: [8, 8, 10, 14, 16, 11, 11, 14],
             6: [15, 12, 13, 11, 15, 15, 11, 12],
             7: [1, 2, 5, 7, 2, 4, 6, 8],
             8: [3, 5, 7, 8, 15, 3, 5, 3],
             9: [2, 2, 2, 2, 3, 5, 3],
             10: [2, 1, 13, 2, 15, 3, 5, 3],
             11: [8, 8, 16, 14, 16, 15, 16, 14],
             12: [3, 7, 7, 8, 16, 7, 16, 3],
             13: [2, 2, 2, 2, 15, 16, 14],
             14: [1, 1, 8, 8, 2, 7, 8, 8],
             15: [8, 8, 8, 2, 7, 8, 9],
             16: [5, 16, 15, 15, 16, 6, 8, 9],
             17: [1, 1, 1, 1, 15, 15, 16, 14],
             18: [10, 11, 11, 8, 7, 3, 5, 3],
             19: [15, 16, 15, 15, 15, 16, 14],
             20: [1, 1, 8, 8, 1, 7, 8, 8]
             }
Sites = len(RSM)
Relations = len(RSM[0])
Qeps = len(queryPlan)
N = len(queryPlan[1])
CLPC = Db_util2.ClpcOfDatabase
Sr = Db_util2.selectivitytable

print("Number of sites:", Sites, "\nNumber of Relations", Relations, "\nNumber of QEP:", Qeps)
print()

# print("Table row:", Db_util2.detail)

# Row Count of each table/relation in a separate array R
R = []

for item in Db_util2.detail:
    R.append(Db_util2.detail[item])
# print("R er value hochhe: ", R)

def queryAffinityCost(queryPlan):
    ret = []
    for i in range(1, Qeps + 1):
        QAC = 0
        sitesRequired = set(queryPlan[i])
        #print(i, "site: ", sitesRequired)
        for relations in sitesRequired:
            
            # Ki is the summation of how many times the particular site is used in a query 
            Ki = queryPlan[i].count(relations)
            #print(relations, "table", Ki)
            QAC = QAC + (Ki * (N - Ki)) / (N * N)
        QAC = round(QAC, 4)
        ret.append(QAC)
        # print(QAC)
    return ret

# QEP Query Execution Plan
# QEP contains the individual query plan (single array)
def QEP_util(QEP):
    qlcs = []
    # total is the total number of rows/tuples
    total = sum(R)
    for site in QEP:
        value = 0
        ratio = 0
        for j in range(len(QEP)):
            if site == QEP[j]:
                ratio = 0
                value = value + ratio
            else:
                ratio = R[j - 1] / total
                value = value + ratio
        qlcs.append(round(value, 4))
    # returns the minimum of qlcs array. qlcs array contains the ratio of the rows of each table.
    return min(qlcs)


def queryLocalizationCost(QEP):
    ret = []
    for i in range(1, len(QEP) + 1):
        ret.append(QEP_util(QEP[i]))
    return ret


def RPC(relation):  # the relation number
    # selectivity of relation (Number of tuples participating in join or semi join) is required
    total = sum(R)
    Sr_ = Sr[relation - 1]
    rpc = R[relation - 1] * Sr_ / total
    return rpc


def RLPC(qp):
    sum_ = 0
    for i in qp:
        currentsite = RSM[i - 1]  # array of qp in sites
        values = []
        # print('currentsite', currentsite)
        for j in range(len(currentsite)):
            if currentsite[j] == 1:
                values.append((RPC(j + 1)))
        sum_ = sum_ + max(values)
    return sum_


def localProcessingCost(queryplan):
    total_ = []
    for i in range(1, len(queryplan) + 1):
        lpc = RLPC(queryplan[i]) + CLPC
        total_.append(round(lpc, 4))
    return total_


print("================================================")
print('QAC:', queryAffinityCost(queryPlan))
# print('len', len(queryAffinityCost(queryPlan)))
print('QLC:', queryLocalizationCost(queryPlan))
# print('len', (len(queryLocalizationCost(queryPlan))))
print('LPC:', localProcessingCost(queryPlan))
# print('len', (len(localProcessingCost(queryPlan))))

QAC = queryAffinityCost(queryPlan)
QLC = queryLocalizationCost(queryPlan)
LPC = localProcessingCost(queryPlan)
costDict = dict()
for i in range(len(QAC)):
    costDict[i + 1] = []
    costDict[i + 1].append(QAC[i])
    costDict[i + 1].append(QLC[i])
    costDict[i + 1].append(LPC[i])
# for i in costDict:
#     print("Cost Dictionary:",i,costDict[i])
print("cost dictionary=", costDict)
