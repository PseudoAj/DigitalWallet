#!/usr/bin/env python

# title           : main.py
# description     : This is the main to build model and check for the straming transactions
# author		  : Ajay Krishna Teja Kavuri
# date            : 20161107
# version         : 1.0
# ==============================================================================

# libraries
import time
import sys
from buildModel import *
import networkx as nx

# ==============================================================================

# This class is going to retrive data from streaming file
# Generate output for the features
class main():

    # Initialize the class and build
    def __init__(self,btchPymntsPath,strmPymntsPath,f1Out,f2Out,f3Out):
        # ===== Variables =====
        # Path of the batch data
        self.btchPymntsPath = btchPymntsPath

        # Path of the stream data
        self.strmPymntsPath = strmPymntsPath

        # Maximum tokens in the data
        self.maxTkns = 5

        # Bad data pckts count
        self.badTrns = 0

        # Maximum degree to be allowed
        self.maxDegree = 4

        # variable for output files
        self.f1Out = f1Out
        self.f2Out = f2Out
        self.f3Out = f3Out

        # ===== Functions =====
        # Build the model
        thisModel = buildModel(btchPymntsPath)

        # Call the build operation
        self.nodesDict,self.treeDict, self.nxGraph = thisModel.build()

        # ===== Debug =====
        # Debug Statement
        print "==== Checking stream ====="

    # Get the degree from the nx graph instead
    def getDgre(self,graph,start, goal):
        # Try to find the node path length
        try:
            curDgre = nx.shortest_path_length(graph,source=start,target=goal)
            return curDgre, True
        # Catch exception
        except nx.NetworkXNoPath, nx.NetworkXError:
            return 0, False

    # Check for the neighbors using a BFS search
    def bfsPths(self, graph, start, goal):
        queue = [(start, [start])]
        while queue:
            # Get the vertex and add to path
            (vertex, path) = queue.pop(0)

            curSet = set()
            curDict = graph.get((vertex))

            for node in curDict.keys():
                curSet.add(node)

            for next in curSet - set(path):
                if next == goal:
                    yield path + [next]
                else:
                    queue.append((next, path + [next]))

    def shrtstPth(self, graph, start, goal):
        try:
            return (len(next(self.bfsPths(graph, start, goal)))-1),True
        except StopIteration:
            return 0,False

    # Write the line to the mentioned file
    def writeFltrdData(self,fname,thisDataStr):
        with open(fname, 'a+') as outFile:
            outFile.write(thisDataStr)

    # simple function to get time in millisecs
    def getTimeInMilli(self):
        return float(round(time.time() * 1000))

    # Funtion to filter the transaction
    def fltr(self,trnsctn):
        # Tokenize the data
        tkns = trnsctn.split(",")

        # check for the tkns length
        if not len(tkns) >= self.maxTkns:
            # Increament the counter
            self.badTrns+=1
            # return nothing
            return -1
        else:
            # return tkns
            return tkns

    # Function to get one degree neighbors
    def isOneDegNghbrs(self,sndr,rcvr):
        # Algorithm:
        # If sndr in keys
        #   If rcvr in neighbors of sndr
        #       return true
        if self.treeDict.get(sndr):
            if self.treeDict[sndr].get(rcvr):
                # print str(rcvr)+" exists in "+str(sndr)+" tree"
                return True
            else:
                return False
        else:
            return 0,False

    # Stream the transactions
    def strmTrns(self):
        # For each transactions in stream payment
        with open(self.strmPymntsPath) as strmFle:
            # Ignore the header
            strmFle.readline()
            # For each trnsctn in strmFle
            for trnsctn in strmFle:
                # Intialize as unverified by default
                curOt1 = "unverified"
                curOt2 = "unverified"
                curOt3 = "unverified"

                # Filter the data before running analysis
                # Get the tokenized data
                tkns = self.fltr(trnsctn)

                # Extract the cuurent nodes
                curSndr = int(tkns[1].strip())
                curRcvr = int(tkns[2].strip())

                # Ignore any looped transactions
                if curSndr == curRcvr:
                    continue

                # Optimization 1:
                # If rcvr not in nodes:
                # print unverified
                if not self.nodesDict.get(curRcvr):
                    curOt1 = "unverified"
                    curOt2 = "unverified"
                    curOt3 = "unverified"

                # Optimization 2:
                # If rcvr not in tree[sendr]:
                # print unverified
                else:
                    if self.isOneDegNghbrs(curSndr,curRcvr):
                        curOt1 = "trusted"
                        curOt2 = "trusted"
                        curOt3 = "trusted"
                    # Run for the next iterations
                    else:
                        # Call nx graph
                        curDegree, pthExsts = self.getDgre(self.nxGraph,curSndr,curRcvr)

                        # My custom implementation
                        # curDegree, pthExsts = self.shrtstPth(self.treeDict,curSndr,curRcvr)
                        # Check if the pthExsts
                        if pthExsts:
                            # Check if the degree is 2
                            if curDegree >= 2 and curDegree < 4:
                                curOt1 = "unverified"
                                curOt2 = "trusted"
                                curOt3 = "trusted"
                            if curDegree == 4:
                                curOt1 = "unverified"
                                curOt2 = "unverified"
                                curOt3 = "trusted"

                # Write the output to the files
                self.writeFltrdData(self.f1Out,curOt1+'\n')
                self.writeFltrdData(self.f2Out,curOt2+'\n')
                self.writeFltrdData(self.f3Out,curOt3+'\n')

                # Debug
                # print self.treeDict
                # print curOt1+", "+curOt2+", "+curOt3
                # print "Current transaction: "+str(trnsctn)+" Sender: "+str(curSndr)+" Reciever: "+str(curRcvr)
                # raw_input()

        # Debug
        # print curOt1+", "+curOt2+", "+curOt3
        # nx.draw(self.nxGraph)
        # plt.show()



# Main function for triggering the methods
if __name__ == "__main__":
    # Read arguments if passed
    if len(sys.argv) > 1:
        btchPymnt = str(sys.argv[1])
        strmPymnt = str(sys.argv[2])
        f1Out = str(sys.argv[3])
        f2Out = str(sys.argv[4])
        f3Out = str(sys.argv[5])
    else:
        # Variable for the batch path
        btchPymnt = "../paymo_input/batch_payment.txt"
        # btchPymnt = "../paymo_input/test_batch.txt"

        # Variable for the stream path
        # strmPymnt = "../paymo_input/stream_payment.txt"
        # strmPymnt = "../paymo_input/stream_oneSec.txt"
        strmPymnt = "../paymo_input/test_stream.txt"

        # Output file path
        f1Out = "../paymo_output/output1.txt"
        f2Out = "../paymo_output/output2.txt"
        f3Out = "../paymo_output/output3.txt"

    # Call for the class
    thisRun = main(btchPymnt,strmPymnt,f1Out,f2Out,f3Out)

    # Run the stream
    thisRun.strmTrns()
