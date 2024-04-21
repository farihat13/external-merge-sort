#ifndef _LOSER_TREE_H_
#define _LOSER_TREE_H_


#include "Record.h"
#include <algorithm>
#include <cassert>
#include <climits>
#include <cstring>
#include <functional>
#include <iostream>
#include <vector>


class LoserTree {
  private:
    std::vector<Record *> loserTree;
    std::vector<int> indices;
    Record *dummy;

  public:
    LoserTree() { dummy = getMaxRecord(); }

    void printTree() {
        // #if defined(_DEBUG)
        //         printf("loser tree: ");
        //         for (int i = 0; i < loserTree.size(); i++) {
        //             if (i < indices.size())
        //                 printf("[%d]:%s(%d) ", i, loserTree[i]->repr(), indices[i]);
        //             else
        //                 printf("[%d]:%s ", i, loserTree[i]->repr());
        //         }
        printf("\n");
        // #endif
    }


    void constructTree(std::vector<Run> &inputs) {
        int nInternalNodes = inputs.size();
        if (inputs.size() % 2 == 1) {
            nInternalNodes++;
        }
        this->loserTree.resize(nInternalNodes * 2, dummy);
        this->indices.resize(nInternalNodes, -1);


        // printv("nInternalNodes: %d\n", nInternalNodes);
        // printv("size of loserTree: %zu\n", this->loserTree.size());

        // Set the leaf values using the first elements of the lists
        for (int i = nInternalNodes; i < nInternalNodes + inputs.size(); i++) {
            this->loserTree[i] = inputs[i - nInternalNodes].getHead();
            if (this->loserTree[i] == NULL) {
                printf("Did not expect empty list\n ");
                exit(1);
            }
        }
        if (inputs.size() % 2 == 1) {
            // Add a dummy node to make the number of leaves even
            this->loserTree[nInternalNodes * 2 - 1] = dummy;
        }
        printTree();

        // Construct the loser tree from leaves up
        for (int i = nInternalNodes; i < nInternalNodes * 2; i++) {
            Record *winner = this->loserTree[i];
            int parIdx = i; // parent index
            int winnerIdx = i;
            while (parIdx >= 0) {
                parIdx /= 2;
                if (isRecordMax(this->loserTree[parIdx])) {
                    // if (loserTree[parIdx]->val == INT_MAX) {
                    this->loserTree[parIdx] = winner;
                    this->indices[parIdx] = winnerIdx;
                    break;
                } else if (*winner > *this->loserTree[parIdx]) {
                    std::swap(this->loserTree[parIdx], winner);
                    std::swap(this->indices[parIdx], winnerIdx);
                }
                if (parIdx == 0) {
                    break;
                }
            }
        }
        printvv("Constructed loser tree, size: %zu (#internal %zu)\n", loserTree.size(),
                nInternalNodes);
        // printTree();
    }

    Record *getNext() {
        // printv("\t\tbefore prop: ");
        // printTree();

        Record *currWinner = loserTree[0];
        if (isRecordMax(currWinner)) {
            // if (currWinner->val == INT_MAX) {
            printvv("No more winners\n");
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
        // printv("\n\twinner: %s, winningIdx: %d\n", currWinner->repr(), winningIdx);

        // Update the tree with the next value from the list corresponding
        // to the found leaf
        loserTree[winningIdx] = loserTree[winningIdx]->next;
        if (loserTree[winningIdx] == NULL) {
            loserTree[winningIdx] = dummy;
        }
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
        Record *winner = loserTree[winnerIdx];

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

    Record *mergeKLists(std::vector<Run> &lists) {
        constructTree(lists);
        Record *head = new Record();
        Record *current = head;
        while (true) {
            Record *winner = getNext();
            if (winner == NULL) {
                break;
            }
            // printf("winner: %d\n", winner->val);
            current->next = winner;
            current = current->next;
        }
        return head->next;
    }
}; // class LoserTree


#endif // _LOSER_TREE_H_