#ifndef _LOSER_TREE_H_
#define _LOSER_TREE_H_


#include "Record.h"
#include "RunStreamer.h"
#include "Storage.h"
#include <algorithm>
#include <cassert>
#include <climits>
#include <cstring>
#include <functional>
#include <sstream>
#include <vector>


class LoserTree {
  private:
    std::string name; // for debugging
    std::vector<RunStreamer *> loserTree;
    std::vector<int> indices;
    RunStreamer *dummy;
    Run *dummyRun;

    bool isMax(RunStreamer *r) { return isRecordMax(r->getCurrRecord()); }

  public:
    LoserTree() {
        dummyRun = new Run(getMaxRecord(), 1);
        dummy = new RunStreamer(StreamerType::INMEMORY_RUN, dummyRun);
        // set the current time reabable format as name
        std::time_t ct = std::time(0);
        name = std::string(ctime(&ct));
        name = name.substr(4, name.size() - 10);
    }

    ~LoserTree() {
        for (RowCount i = 0; i < loserTree.size(); i++) {
            if (loserTree[i] != dummy) { delete loserTree[i]; }
        }
        printv("\t\t\t\tDeleted loser tree (%s)\n", name.c_str());
        // NOTE: do not delete dummyRun, it uses maxRecord which is a global
        dummyRun->setHead(nullptr);
        delete dummyRun;
        delete dummy;
    }

    // print the tree
    std::string repr() {
        std::stringstream ss;
        ss << "loser tree (" << name << "):";
        for (size_t i = 0; i < loserTree.size(); i++) {
            if (i < indices.size())
                ss << "[" << i << "]:" << loserTree[i]->repr() << "(" << indices[i] << ") ";
            else
                ss << "[" << i << "]:" << loserTree[i]->repr() << " ";
        }
        std::string str = ss.str();
        return str;
    }
    void printTree() { printv("%s\n", repr().c_str()); }


    void constructTree(std::vector<Run> &inputs) {
        std::vector<RunStreamer *> runStreamers;
        for (size_t i = 0; i < inputs.size(); i++) {
            runStreamers.push_back(new RunStreamer(StreamerType::INMEMORY_RUN, &inputs[i]));
        }
        constructTree(runStreamers);
    }

    void constructTree(std::vector<RunStreamer *> &inputs) {
        int nInternalNodes = inputs.size();
        if (inputs.size() % 2 == 1) { nInternalNodes++; }
        this->loserTree.resize(nInternalNodes * 2, dummy);
        this->indices.resize(nInternalNodes, -1);

        // Set the leaf values using the first elements of the lists
        for (size_t i = nInternalNodes; i < nInternalNodes + inputs.size(); i++) {
            this->loserTree[i] = inputs[i - nInternalNodes];
            if (this->loserTree[i] == NULL) {
                printvv("ERROR: Did not expect empty list\n");
                exit(1);
            }
        }
        if (inputs.size() % 2 == 1) {
            // Add a dummy node to make the number of leaves even
            this->loserTree[nInternalNodes * 2 - 1] = dummy;
        }
        // printTree();

        // Construct the loser tree from leaves up
        for (int i = nInternalNodes; i < nInternalNodes * 2; i++) {
            RunStreamer *winner = this->loserTree[i];
            int parIdx = i; // parent index
            int winnerIdx = i;
            while (parIdx >= 0) {
                parIdx /= 2;
                if (isMax(this->loserTree[parIdx])) {
                    // if (loserTree[parIdx]->val == INT_MAX) {
                    this->loserTree[parIdx] = winner;
                    this->indices[parIdx] = winnerIdx;
                    break;
                } else if (*winner > *this->loserTree[parIdx]) {
                    std::swap(this->loserTree[parIdx], winner);
                    std::swap(this->indices[parIdx], winnerIdx);
                }
                if (parIdx == 0) { break; }
            }
        }
        // printvv("Constructed loser tree (%s), size: %zu (#internal %zu)\n", name.c_str(),
        //         loserTree.size(), nInternalNodes);
        // printTree();
    }

    Record *getNext() {
        // printv("\t\tbefore prop: ");
        // printTree();

        Record *currWinner = loserTree[0]->getCurrRecord();
        if (isRecordMax(currWinner)) {
            // if (currWinner->val == INT_MAX) {
            printv("\t\t\t\tNo more winners\n");
            return NULL;
        }

        // Trace down the tree from the root to find the winning leaf;
        // int winningIdx = 1;
        // for (int i = loserTree.size() / 2; i < loserTree.size(); i++) {
        //     if (loserTree[i] == currWinner) {
        //         winningIdx = i;
        //         break;
        //     }
        // }
        int winningIdx = indices[0];
        // printv("\n\twinner: %s, winningIdx: %d\n", currWinner->reprKey(), winningIdx);

        // Update the tree with the next value from the list corresponding
        // to the found leaf
        // loserTree[winningIdx] = loserTree[winningIdx]->next;
        Record *nextRecord = loserTree[winningIdx]->moveNext();
        if (nextRecord == NULL) { loserTree[winningIdx] = dummy; }
        // if (loserTree[winningIdx] == NULL) {
        //     loserTree[winningIdx] = dummy;
        // } else {
        //     loserTree[winningIdx]->moveNext(); // move to the next record
        // }
        loserTree[0] = dummy;
        indices[0] = -1;
        // printv("\t\tupdated leaf: ");
        // printTree();

        int parIdx = winningIdx / 2;
        int leftIdx = parIdx * 2;
        int rightIdx = leftIdx + 1;
        int loserIdx = *loserTree[leftIdx] < *loserTree[rightIdx] ? rightIdx : leftIdx;
        int winnerIdx = loserTree[leftIdx] == loserTree[loserIdx] ? rightIdx : leftIdx;
        loserTree[parIdx] = loserTree[loserIdx];
        indices[parIdx] = loserIdx;
        RunStreamer *winner = loserTree[winnerIdx];

        while (parIdx >= 0) {
            parIdx /= 2;
            // printv("\t\tparent: [%d]:%s, winner: %s, winneBigger %d winnerSmaller %d\n", parIdx,
            //        loserTree[parIdx]->repr(), winner->repr(), *winner > *loserTree[parIdx],
            //        *winner < *loserTree[parIdx]);
            if (*winner > *loserTree[parIdx]) {
                // if (winner->val > loserTree[parIdx]->val) {
                std::swap(winner, loserTree[parIdx]);
                std::swap(winnerIdx, indices[parIdx]);
            }
            if (parIdx == 0) {
                loserTree[parIdx] = winner;
                indices[parIdx] = winnerIdx;
                break;
            }
        }
        // printv("\t\tafter prop: ");
        // printTree();

        return currWinner;
    }

    Record *mergeKLists(std::vector<RunStreamer *> &lists) {
        constructTree(lists);
        Record *head = new Record();
        Record *current = head;
        while (true) {
            Record *winner = getNext();
            if (winner == NULL) { break; }
            // printf("winner: %d\n", winner->val);
            current->next = winner;
            current = current->next;
        }
        return head->next;
    }
}; // class LoserTree


#endif // _LOSER_TREE_H_