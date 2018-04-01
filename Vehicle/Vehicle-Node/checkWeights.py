import numpy as np
import os
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
            print "bad", epoch
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


def expected(values, expected):
    values = np.asarray(values)
    numGood = 0

    for value in values:
        if abs(value - expected) < 0.0000001:
            numGood += 1
        else:
            # print "bad, expected", expected, "got:", value
            pass

    print "median:\t", np.median(values)
    print "average:\t", np.average(values)
    print "variance:\t", np.var(values)
    print "percentGood:\t", numGood * 100.0 / len(values)


def analyzeOverall(results, numFiles):
    values = extract(results)

    expectedValue = np.average(np.arange(numFiles))
    expected(values, expectedValue)


def writeToFile(root, type, data):
    path = root + type + ".txt"
    fd = open(path, 'w')
    fd.write(str(data))
    fd.close()


def main():
    root = "LogOutput/"
    results = {}
    files = os.listdir(root)
    numFiles = len(files)
    for file in files:
        path = root + file + "/output.log"
        print file
        fd = open(path)

        result = getValuesFromFile(fd)
        updateResults(results, result)

        fd.close()

    opath = "results/1w80m/" + str(numFiles) + "/"
    if os.path.exists(opath):
        shutil.rmtree(opath)
    os.makedirs(opath)

    writeToFile(opath, "raw", results)
    pruneResults(results, numFiles)
    analyzeOverall(results, numFiles)

    # now check how many are different


if __name__ == "__main__":
    main()
