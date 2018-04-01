import matplotlib.pyplot as plt
import numpy as np
import os
import pprint
import re
import shutil

finalRE = re.compile("FINAL RESULT:")
avgRe = re.compile("(?<=avg, item=ids]=)([0-9^\.]+)")
sumRe = re.compile("(?<=sum, item=ids]=)([0-9^\.]+)")
epochRe = re.compile("((?<=01T)[0-6][0-9]:[0-6][0-9]:[0-6][0-9])")


def getValuesFromFile(fd):
    results = {}
    for line in fd:
        match = finalRE.findall(line)

        if match:
            assert 1 == len(match)
            epoch = epochRe.findall(line)[0]
            avg = avgRe.findall(line)
            sum = sumRe.findall(line)
            assert epoch not in results
            if not avg:
                continue
            results[epoch] = float(avg[0])

    return results


def updateResults(results, nr):
    for epoch in nr:
        if epoch in results:
            results[epoch].append(nr[epoch])
        else:
            results[epoch] = [nr[epoch]]


def pruneResults(results, numNodes):
    toRemove = []
    for epoch in results:
        if len(results[epoch]) != numNodes:
            toRemove.append(epoch)

    print "removed:", len(toRemove), "of", len(results)
    print sorted(toRemove)
    for epoch in toRemove:
        del results[epoch]


def extract(results):
    all = []
    for epoch in results:
        all.extend(results[epoch])

    return all


def calcEV(numFiles):
    return np.average(np.arange(numFiles))


def expected(values, numFiles):
    values = np.asarray(values)
    numGood = 0
    expected = calcEV(numFiles)

    diffs = []
    for value in values:
        diffs.append(abs(value - expected) * 100.0 / expected)

    return [np.median(values), np.average(values), np.var(values), np.average(diffs), len(values) * 100.0 / numFiles]


def analyzeEpochs(results, numFiles):
    # find out what the spread is *before* prune
    output = {}
    for epoch in results:
        output[epoch] = expected(results[epoch], numFiles)

    return output


def writeToFile(root, type, data):
    path = root + type + ".txt"
    fd = open(path, 'w')
    fd.write(str(data))
    fd.close()


def parseOnlyFullEpochs(root):
    results = {}
    files = os.listdir(root)
    numFiles = len(files)
    for file in files:
        path = root + file + "/output.log"
        fd = open(path)

        result = getValuesFromFile(fd)
        updateResults(results, result)

        fd.close()

    # prune all that don't have expected number of members in their epoch
    # pruneResults(results, numFiles)

    # now just go and parse the clusterhead file
    clusterhead = {}
    files = os.listdir(root)
    for file in files:
        if "CLUSTERHEAD" not in file:
            continue
        path = root + file + "/output.log"
        fd = open(path)

        result = getValuesFromFile(fd)
        updateResults(clusterhead, result)

        fd.close()

    # finally output just the clusterhead epochs will all members
    output = {}
    for epoch in clusterhead:
        if epoch in results:
            output[epoch] = clusterhead[epoch]

    return output, results


def getSortedError(results, which):
    arr = []
    for epoch in results:
        arr.append((epoch, results[epoch][which]))

    return sortTups(arr)


def sortTups(arr):
    sarr = sorted(arr, key=lambda x: x[0])
    # print sarr
    # now get just the values
    output = []
    for tup in sarr:
        output.append(tup[1])

    return output


def main():
    root = "LogOutput/"
    files = os.listdir(root)
    numFiles = len(files)

    results, fullResults = parseOnlyFullEpochs(root)

    # set up the output
    opath = "results/1w80m/" + str(numFiles) + "/"
    if os.path.exists(opath):
        shutil.rmtree(opath)
    os.makedirs(opath)

    pp = pprint.PrettyPrinter(indent=2)
    # print "going to pretty print"
    cData = analyzeEpochs(results, numFiles)
    fullData = analyzeEpochs(fullResults, numFiles)
    sortedEData = getSortedError(cData, 3)
    percentParticipating = getSortedError(fullData, 4)
    x = np.arange(len(sortedEData))

    fullParticipationIdx = percentParticipating.index(100.0)

    plt.scatter(x, sortedEData, c='g', label="error")
    plt.scatter(x, percentParticipating, c='r', label="participating")
    plt.legend()
    plt.show()

    print "data:, first idx:", fullParticipationIdx
    print np.average(sortedEData[fullParticipationIdx:])
    print np.var(sortedEData[fullParticipationIdx:])
    writeToFile(opath, "chead", results)
    writeToFile(opath, "full", fullResults)

    # pruneResults(results, numFiles)
    # analyzeOverall(results, numFiles)

    # now check how many are different


if __name__ == "__main__":
    main()
