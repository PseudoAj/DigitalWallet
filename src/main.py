#!/usr/bin/env python

# title           : main.py
# description     : This is the main to build model and check for the straming transactions
# author		  : Ajay Krishna Teja Kavuri
# date            : 20161107
# version         : 1.0
# ==============================================================================

# libraries
import time
from buildModel import *

# ==============================================================================

# This class is going to retrive data from streaming file
# Generate output for the features
class main():

    # Initialize the class and build
    def __init__(self,btchPymntsPath,strmPymntsPath):
        # ===== Variables =====
        # Path of the batch data
        self.btchPymntsPath = btchPymntsPath

        # Path of the stream data
        self.strmPymntsPath = strmPymntsPath

        # Maximum tokens in the data
        self.maxTkns = 5

        # Bad data pckts count
        self.badTrns = 0

        # ===== Functions =====
        # Build the model
        thisModel = buildModel(btchPymntsPath)

        # Call the build operation
        self.nodesDict,self.treeDict = thisModel.build()

        # ===== Debug =====
        # Debug Statement
        print "Checking stream..."

    # simple function to get time in millisecs
    def getTimeInMilli(self):
        return int(round(time.time() * 1000))

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
            return False

    # Stream the transactions
    def strmTrns(self):
        # For each transactions in stream payment
        with open(self.strmPymntsPath) as strmFle:
            # Ignore the header
            strmFle.readline()
            # For each trnsctn in strmFle
            for trnsctn in strmFle:
                # Filter the data before running analysis
                # Get the tokenized data
                tkns = self.fltr(trnsctn)

                # Extract the cuurent nodes
                curSndr = int(tkns[1].strip())
                curRcvr = int(tkns[2].strip())

                # Find one degree neighbors
                self.isOneDegNghbrs(curSndr,curRcvr)

                # Debug
                # print "Current transaction: "+str(trnsctn)+" Sender: "+str(curSndr)+" Reciever: "+str(curRcvr)
                # raw_input()


# Main function for triggering the methods
if __name__ == "__main__":
    # Variable for the batch path
    btchPymnt = "../paymo_input/batch_payment.csv"
    tstBtch = "../paymo_input/test_batch.csv"
    # Variable for the stream path
    strmPymnt = "../paymo_input/stream_payment.csv"
    tstStrm = "../paymo_input/test_stream.csv"

    # Call for the class
    thisRun = main(btchPymnt,strmPymnt)

    # Run the stream
    thisRun.strmTrns()
