import sys
import re
import numpy as np
from collections import Counter

uuidRe = re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
sendMsgRE = re.compile("OutSocketManager trying to send")
recvMsgRE = re.compile("NetworkLayer message . from: . got")
commitRe = re.compile("Consensus commit")
abortRe = re.compile("Consensus abort ")
abortFollowRe = re.compile("Consensus abort null")
errorRe = re.compile("ERROR")
epochRe = re.compile(":[0-6][0-9]Z")
startRoundRe = re.compile("start new round")


def countOccurOnce(lst): 
    cnt = Counter(lst)
    print [k for k, v in cnt.iteritems() if v == 1]

def parse(fd): 
    sendIds = []
    recvIds = []
    clead = []
    cfollow = []
    alead = []
    tupMap = {}
    errors = []
    epochsMap = {}

    for line in fd: 
        uuids = uuidRe.findall(line)
        isSend = sendMsgRE.findall(line)
        isRecv = recvMsgRE.findall(line)
        isCommit = commitRe.findall(line)
        isAbort = abortRe.findall(line)
        isAbortFollow = abortFollowRe.findall(line)
        isError = errorRe.findall(line)
        isStartRound = startRoundRe.findall(line)
        epoch = epochRe.findall(line)

        if isSend:
            sendIds.append(uuids[0])
        elif isRecv:
            recvIds.append(uuids[0])
        elif isCommit:
            assert(len(uuids) == 2)

            # add both
            clead.append(uuids[0])
            cfollow.append(uuids[1])

            # add to map
            pair = (uuids[0], uuids[1])
            if pair in tupMap: 
                tupMap[pair] = tupMap[pair] + 1
            else: 
                tupMap[pair] = 1

            reversePair = (uuids[1], uuids[0])
            assert(reversePair not in tupMap)

        elif isAbort:
            assert(len(uuids) == 1)

            alead.append(uuids[0])
        elif isError:
            errors.append(isError)

        elif isStartRound:
            if len(epoch) != 1: 
                print line
            assert(len(epoch) == 1)
            e = epoch[0]
            if e in epochsMap: 
                epochsMap[e] = epochsMap[e] + 1
            else:
                epochsMap[e] = 1

        assert(not isAbortFollow)

    print "bad sends that got commited"
    badSends = np.setdiff1d(sendIds, recvIds)
    print set(badSends) & set(set(clead) | set(cfollow))

    print "tups:", len(tupMap)
    if len(tupMap) > 0: 
        print "min:", tupMap[min(tupMap, key=tupMap.get)]
        print "max:", tupMap[max(tupMap, key=tupMap.get)]

    print "eps:", len(epochsMap)
    print epochsMap
    if len(epochsMap) > 0: 
        print "min:", epochsMap[min(epochsMap, key=epochsMap.get)]
        print "max:", epochsMap[max(epochsMap, key=epochsMap.get)]

    for key in tupMap:
        val = tupMap[key]
        if val != 2: 
            print key, ":", val
    
    print "num errors:", len(errors)
    print "num aborts:", len(alead)

def main():
    fd = open(sys.argv[1])
    parse(fd)

if __name__ == "__main__":
    main()
