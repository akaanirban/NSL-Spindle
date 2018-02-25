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

    for line in fd: 
        uuids = uuidRe.findall(line)
        isSend = sendMsgRE.findall(line)
        isRecv = recvMsgRE.findall(line)
        isCommit = commitRe.findall(line)
        isAbort = abortRe.findall(line)
        isAbortFollow = abortFollowRe.findall(line)
        isError = errorRe.findall(line)

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

        assert(not isAbortFollow)

    print "bad sends that got commited"
    badSends = np.setdiff1d(sendIds, recvIds)
    print set(badSends) & set(set(clead) | set(cfollow))

    print "tups:", len(tupMap)
    print "min:", tupMap[min(tupMap, key=tupMap.get)]
    print "max:", tupMap[max(tupMap, key=tupMap.get)]
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
