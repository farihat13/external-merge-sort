#include <cassert>
#include <climits>
#include <cstring>
#include <iostream>
#include <vector>

using namespace std;

#define RECORD_SIZE 4
#define RECORD_KEY_SIZE 2


bool verbose = true;
#define printv(...)                                                            \
    if (verbose)                                                               \
    printf(__VA_ARGS__)

void gen_a_record(char *s, const int len) {
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
}

struct Record {
    int val;
    char *data;
    Record *next;
    Record(int val) : val(val), next(NULL) {}
    Record() {
        data = new char[RECORD_SIZE];
        gen_a_record(data, RECORD_SIZE);
        val = std::stoi(data);
    }
    Record(char *data) : data(data) {}
    bool operator<(const Record &other) const {
        return std::strncmp(data, other.data, RECORD_KEY_SIZE) < 0;
    }
};

class LoserTree {
  private:
    vector<Record *> loserTree;
    Record *dummy = new Record(INT_MAX);

  public:
    void printTree() {
        if (verbose) {
            printf("loser tree: ");
            for (int i = 0; i < loserTree.size(); i++) {
                printf("[%d]:%d ", i, loserTree[i]->val);
            }
            printf("\n");
        }
    }

    void propagateToParent(vector<Record *> &lT, Record *winner, int i) {
        int parIdx = i; // parent index
        while (parIdx >= 0) {
            parIdx /= 2;
            printv("\t\tparent: [%d]:%d -> \n", parIdx,
                   lT[parIdx] ? lT[parIdx]->val : INT_MAX);
            if (lT[parIdx] == NULL || lT[parIdx]->val == INT_MAX) {
                lT[parIdx] = winner;
                break;
            }
            if (winner->val > lT[parIdx]->val) {
                printv("\t\t\tn: %d, w %d\n", lT[parIdx]->val, winner->val);
                std::swap(lT[parIdx], winner);
            }
            if (parIdx == 0) {
                break;
            }
        }
    }


    void propagate(vector<Record *> &lT, int winnerIdx, bool construct = true) {
        Record *winner = lT[winnerIdx];
        int parIdx = winnerIdx; // parent index
        while (parIdx >= 0) {
            parIdx /= 2;
            printv("\t\tparent: [%d]:%d, winner: %d \n", parIdx,
                   lT[parIdx]->val, winner->val);
            if (lT[parIdx]->val == INT_MAX) {
                lT[parIdx] = winner;
                if (construct)
                    break;
            } else if (winner->val > lT[parIdx]->val) {
                std::swap(lT[parIdx], winner);
            }
            if (parIdx == 0) {
                break;
            }
        }
    }


    void constructTree(vector<Record *> &inputs) {
        int nInternalNodes = inputs.size();
        if (inputs.size() % 2 == 1) {
            nInternalNodes++;
        }
        loserTree.resize(nInternalNodes * 2, dummy);

        printv("nInternalNodes: %d\n", nInternalNodes);
        printv("size of loserTree: %zu\n", loserTree.size());

        // Set the leaf values using the first elements of the lists
        for (int i = nInternalNodes; i < nInternalNodes + inputs.size(); i++) {
            loserTree[i] = inputs[i - nInternalNodes];
            if (loserTree[i] == NULL) {
                printf("Did not expect empty list\n ");
                exit(1);
            }
        }
        if (inputs.size() % 2 == 1) {
            // Add a dummy node to make the number of leaves even
            loserTree[nInternalNodes * 2 - 1] = dummy;
        }
        printTree();

        // Construct the loser tree from leaves up
        for (int i = nInternalNodes; i < nInternalNodes * 2; i++) {
            Record *winner = loserTree[i];
            int parIdx = i; // parent index
            while (parIdx >= 0) {
                parIdx /= 2;
                if (loserTree[parIdx]->val == INT_MAX) {
                    loserTree[parIdx] = winner;
                    break;
                } else if (winner->val > loserTree[parIdx]->val) {
                    std::swap(loserTree[parIdx], winner);
                }
                if (parIdx == 0) {
                    break;
                }
            }
        }
        printv("Loser tree constructed\n");
        printTree();
    }

    Record *getNext() {
        printv("\t\tbefore prop: ");
        printTree();

        Record *currWinner = loserTree[0];
        if (currWinner->val == INT_MAX) {
            printf("No more winners\n");
            return NULL;
        }

        // Trace down the tree from the root to find the winning leaf;
        // TODO: store ptr in the tree
        int winningIdx = 1;
        for (int i = loserTree.size() / 2; i < loserTree.size(); i++) {
            if (loserTree[i] == currWinner) {
                winningIdx = i;
                break;
            }
        }
        printv("\n\twinner: %d, winningIdx: %d\n", currWinner->val, winningIdx);

        // Update the tree with the next value from the list corresponding
        // to the found leaf
        loserTree[winningIdx] = loserTree[winningIdx]->next;
        if (loserTree[winningIdx] == NULL) {
            loserTree[winningIdx] = dummy;
        }
        loserTree[0] = dummy;
        printv("\t\tupdated leaf: ");
        printTree();

        int parIdx = winningIdx / 2;
        int leftIdx = parIdx * 2;
        int rightIdx = leftIdx + 1;
        int loserIdx = loserTree[leftIdx]->val < loserTree[rightIdx]->val
                           ? rightIdx
                           : leftIdx;
        int winnerIdx =
            loserTree[leftIdx] == loserTree[loserIdx] ? rightIdx : leftIdx;
        loserTree[parIdx] = loserTree[loserIdx];
        Record *winner = loserTree[winnerIdx];

        while (parIdx >= 0) {
            parIdx /= 2;
            printv("\t\tparent: [%d]:%d, winner: %d \n", parIdx,
                   loserTree[parIdx]->val, winner->val);
            if (winner->val > loserTree[parIdx]->val) {
                std::swap(winner, loserTree[parIdx]);
            }
            if (parIdx == 0) {
                loserTree[parIdx] = winner;
                break;
            }
        }
        printv("\t\tafter prop: ");
        printTree();

        return currWinner;
    }

    Record *mergeKLists(vector<Record *> &lists) {
        constructTree(lists);
        Record *head = new Record(INT_MIN);
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
};


void printList(Record *node) {
    while (node) {
        printf("%d ", node->val);
        node = node->next;
    }
    printf("\n");
}


// Function to create a linked list from a vector of integers
Record *createList(const vector<int> &values) {
    if (values.empty())
        return NULL;
    Record *head = new Record(values[0]);
    Record *current = head;
    for (size_t i = 1; i < values.size(); i++) {
        current->next = new Record(values[i]);
        current = current->next;
    }
    return head;
}

// Function to verify that the list is sorted
bool isSorted(Record *head) {
    Record *current = head;
    while (current && current->next) {
        if (current->val > current->next->val) {
            return false;
        }
        current = current->next;
    }
    return true;
}

// Function to generate random integers
int generateRandomInt(int min, int max) {
    return min + rand() % (max - min + 1);
}

// Function to generate random lists
vector<int> generateRandomList(int minLength, int maxLength, int minValue,
                               int maxValue) {
    int length = generateRandomInt(minLength, maxLength);
    vector<int> result;
    for (int i = 0; i < length; i++) {
        result.push_back(generateRandomInt(minValue, maxValue));
    }
    sort(result.begin(), result.end()); // Ensure the list is sorted
    return result;
}

// test the LoserTree implementation
// check sorted order
// check total number of elements
int automatedTest() {
    verbose = false;
    srand(time(NULL)); // Seed the random number generator

    vector<Record *> lists;
    int numberOfLists = 1000;
    // generateRandomInt(5, 10); // Generate between 5 and 10 lists

    cout << "Generated " << numberOfLists << " random lists:\n";
    int totalElements = 0;
    for (int i = 0; i < numberOfLists; i++) {
        vector<int> values = generateRandomList(
            1, 1000, -325325,
            3543); // Lists of length 0-10 with values between 0 and 100
        totalElements += values.size();
        lists.push_back(createList(values));
        // printList(lists.back());
    }

    // Create LoserTree and merge lists
    LoserTree lt;
    Record *result = lt.mergeKLists(lists);

    // // Print the merged result
    // cout << "Merged list: ";
    // printList(result);

    // Verify that the merged result is correctly sorted
    assert(isSorted(result));
    cout << "Verification passed: The merged list is sorted.\n";
    int count = 0;
    Record *current = result;
    while (current) {
        count++;
        current = current->next;
    }
    cout << "Total number of elements: " << count << "/" << totalElements
         << endl;
    assert(count == totalElements);
    cout << "Verification passed: The merged list contains the correct number "
            "of elements.\n";

    return 0;
}

int main() {
    automatedTest();
    exit(0);

    int arr[] = {4, 3, 6, 8, 1, 5, 7};
    Record *list1 = new Record(4);
    list1->next = new Record(400);
    list1->next->next = new Record(500);

    Record *list2 = new Record(3);
    list2->next = new Record(300);
    list2->next->next = new Record(400);

    Record *list3 = new Record(6);
    list3->next = new Record(600);

    Record *list4 = new Record(8);
    list4->next = new Record(700);
    list4->next->next = new Record(800);

    vector<Record *> lists;
    lists.push_back(list1);
    lists.push_back(list2);
    lists.push_back(list3);
    for (int i = 0; i < 4; i++) {
        Record *list = new Record(i + 1);
        list->next = new Record(i + 10);
        list->next->next = new Record(i + 20);
        list->next->next->next = new Record(i + 30);
        lists.push_back(list);
    }
    int i = 0;
    for (Record *list : lists) {
        list->val = arr[i++];
        printList(list);
    }

    LoserTree lt;
    lt.constructTree(lists);
    verbose = false;
    while (true) {
        Record *winner = lt.getNext();
        if (winner == NULL) {
            printf("No more winners\n");
            break;
        }
        printf("winner: %d\n", winner->val);
    }
    // printf("Done\n");
    // lt.printTree();

    return 0;
}
