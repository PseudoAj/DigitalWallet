#!/usr/bin/env python

# title           : buildModel.py
# description     : The class will build the model to avoid the fraud payments,
#                   basic idea is to build:
#                   1. Hashtable to store all the userId that has ever made a transaction
#                   2. Tree of the nodes with which transactions have happened for each node
# author		  : Ajay Krishna Teja Kavuri
# date            : 20161105
# version         : 1.0
# ==============================================================================

# This is the class to build the model to detect the frauds
class buildModel():

    # Initialize the model
    def __init__(self,btchPymntsPath):
        # ===== Variables =====
        # Path of the batch data
        self.btchPymntsPath = btchPymntsPath
        # self.btchPymntsPath = "../paymo_input/batch_payment.csv"

        # Maximum tokens in the data
        self.maxTkns = 5

        # Bad data pckts count
        self.badTrns = 0

        # ===== Data structures =====
        self.nodesDict = {}

        # Dict that holds the dict of nodes
        self.treeDict = {}

        # ===== Debug =====
        # Debug Statement
        print "Bulding model..."

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

    # This function is responsible for building the hash table of nodes
    def bldNdsHT(self,sndr,rcvr):
        # Add to the dictionary if doesn't aready exists
        # Add the sndr node of current transaction
        if not self.nodesDict.get(sndr):
            self.nodesDict[sndr] = sndr
        # Add the rcvr node of current transaction
        if not self.nodesDict.get(rcvr):
            self.nodesDict[rcvr] = rcvr

    # Build the tree HT
    def bldTrnsTree(self,sndr,rcvr):
        # For each node there is an entree in the main dictionary
        # Further, each node contains a sub-dictionary of values
        # If this is first time tree has seen sndr node
        if self.treeDict.get(sndr):
            # Add the reciever with check
            if not self.treeDict[sndr].get(rcvr):
                # Add to the dict of the
                self.treeDict[sndr][rcvr] = rcvr
        # if doesn't exist, initiate and create the dict
        else:
            # initiate
            self.treeDict[sndr] = {}
            # Add the value
            self.treeDict[sndr][rcvr] = rcvr
        # If this is first time tree has seen rcvr node
        if self.treeDict.get(rcvr):
            # Add the reciever with check
            if not self.treeDict[rcvr].get(sndr):
                # Add to the dict of the
                self.treeDict[rcvr][sndr] = sndr
        # if doesn't exist, initiate and create the dict
        else:
            # initiate
            self.treeDict[rcvr] = {}
            # Add the value
            self.treeDict[rcvr][sndr] = sndr


    # Function to trigger the build operation
    def build(self):
        # Run through the file
        with open(self.btchPymntsPath) as btchFile:
            # Ignore the header
            btchFile.readline()
            # # Run through each transaction
            for trnsctn in btchFile:
                # Filter the data before running analysis
                # Get the tokenized data
                tkns = self.fltr(trnsctn)

                # Extract the cuurent nodes
                curSndr = int(tkns[1].strip())
                curRcvr = int(tkns[2].strip())

                # Build nodes
                self.bldNdsHT(curSndr,curRcvr)
                # Build tree of transactions
                self.bldTrnsTree(curSndr,curRcvr)

                # Debug
                # print "Current transaction: "+str(trnsctn)+" Sender: "+str(curSndr)+" Reciever: "+str(curRcvr)
                # print "Current nodes hash table: "+str(self.nodesDict)
                # print "Curren tree: "+str(self.treeDict)
                # raw_input()
        # Debug
        # print "Bad transactions: "+str(self.badTrns)
        # print len(self.nodesDict)
        # print len(self.treeDict)

        # Return values: dict and trees
        return self.nodesDict,self.treeDict

# Main method to trigger the class
if __name__=="__main__":
    # Initialize the model
    thisModel = buildModel("../paymo_input/batch_payment.csv")

    # Call the build operation
    thisModel.build()
