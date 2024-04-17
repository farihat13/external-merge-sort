#include <algorithm>
#include <cassert>
#include <climits>
#include <cstring>
#include <iostream>
#include <vector>

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

class Record {
  public:
    // int val;
    char *data;
    Record *next;
    // Record(int val) : val(val), next(NULL) {}
    Record() { data = new char[RECORD_SIZE]; }
    Record(char *data) : data(data) {}
    bool operator<(const Record &other) const {
        return std::strncmp(data, other.data, RECORD_KEY_SIZE) < 0;
    }
    bool operator>(const Record &other) const {
        return std::strncmp(data, other.data, RECORD_KEY_SIZE) > 0;
    }
    char *reprKey() {
        char *key = new char[RECORD_KEY_SIZE + 1];
        std::strncpy(key, data, RECORD_KEY_SIZE);
        key[RECORD_KEY_SIZE] = '\0';
        return key;
    }
    char *repr() {
        char *key = new char[RECORD_SIZE + 1];
        std::strncpy(key, data, RECORD_SIZE);
        key[RECORD_SIZE] = '\0';
        return key;
    }
};

class LoserTree {
  private:
    std::vector<Record *> loserTree;
    std::vector<int> indices;
    Record *dummy;

  public:
    LoserTree() {
        dummy = new Record();
        for (int i = 0; i < RECORD_SIZE; i++) {
            dummy->data[i] = '~';
        }
    }

    bool isRecordMax(Record *r) {
        for (int i = 0; i < RECORD_SIZE; i++) {
            if (r->data[i] != '~') {
                return false;
            }
        }
        return true;
    }

    void printTree() {
        if (verbose) {
            printf("loser tree: ");
            for (int i = 0; i < loserTree.size(); i++) {
                if (i < indices.size())
                    printf("[%d]:%s(%d) ", i, loserTree[i]->repr(), indices[i]);
                else
                    printf("[%d]:%s ", i, loserTree[i]->repr());
            }
            printf("\n");
        }
    }

    void constructTree(std::vector<Record *> &inputs) {
        int nInternalNodes = inputs.size();
        if (inputs.size() % 2 == 1) {
            nInternalNodes++;
        }
        this->loserTree.resize(nInternalNodes * 2, dummy);
        this->indices.resize(nInternalNodes, -1);


        printv("nInternalNodes: %d\n", nInternalNodes);
        printv("size of loserTree: %zu\n", this->loserTree.size());

        // Set the leaf values using the first elements of the lists
        for (int i = nInternalNodes; i < nInternalNodes + inputs.size(); i++) {
            this->loserTree[i] = inputs[i - nInternalNodes];
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
        printv("Loser tree constructed\n");
        printTree();
    }

    Record *getNext() {
        printv("\t\tbefore prop: ");
        printTree();

        Record *currWinner = loserTree[0];
        if (isRecordMax(currWinner)) {
            // if (currWinner->val == INT_MAX) {
            printf("No more winners\n");
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
        printv("\n\twinner: %s, winningIdx: %d\n", currWinner->repr(),
               winningIdx);

        // Update the tree with the next value from the list corresponding
        // to the found leaf
        loserTree[winningIdx] = loserTree[winningIdx]->next;
        if (loserTree[winningIdx] == NULL) {
            loserTree[winningIdx] = dummy;
        }
        loserTree[0] = dummy;
        indices[0] = -1;
        printv("\t\tupdated leaf: ");
        printTree();

        int parIdx = winningIdx / 2;
        int leftIdx = parIdx * 2;
        int rightIdx = leftIdx + 1;
        int loserIdx =
            *loserTree[leftIdx] < *loserTree[rightIdx] ? rightIdx : leftIdx;
        int winnerIdx =
            loserTree[leftIdx] == loserTree[loserIdx] ? rightIdx : leftIdx;
        loserTree[parIdx] = loserTree[loserIdx];
        indices[parIdx] = loserIdx;
        Record *winner = loserTree[winnerIdx];

        while (parIdx >= 0) {
            parIdx /= 2;
            printv("\t\tparent: [%d]:%s, winner: %s, winneBigger %d "
                   "winnerSmaller %d\n",
                   parIdx, loserTree[parIdx]->repr(), winner->repr(),
                   *winner > *loserTree[parIdx], *winner < *loserTree[parIdx]);
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
        printv("\t\tafter prop: ");
        printTree();

        return currWinner;
    }

    Record *mergeKLists(std::vector<Record *> &lists) {
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
};


void printList(Record *node) {
    while (node) {
        printf("%s ", node->repr());
        node = node->next;
    }
    printf("\n");
}

// Function to verify that the list is sorted
bool isSorted(Record *head) {
    Record *current = head;
    while (current && current->next) {
        if (*current > *current->next) {
            printf("Error: List is not sorted. %s > %s\n", current->repr(),
                   current->next->repr());
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
Record *generateRandomList(int length) {
    std::vector<Record> result;
    for (int i = 0; i < length; i++) {
        Record *r = new Record();
        gen_a_record(r->data, RECORD_SIZE);
        result.push_back(*r);
    }
    sort(result.begin(), result.end()); // Ensure the list is sorted
    Record *head = new Record();
    Record *current = head;
    for (size_t i = 0; i < result.size(); i++) {
        Record *r = new Record();
        std::strncpy(r->data, result[i].data, RECORD_SIZE);
        current->next = r;
        current = current->next;
    }
    return head->next;
}

// test the LoserTree implementation
// check sorted order
// check total number of elements
int automatedTest() {
    verbose = false;
    // srand(0);
    srand(time(NULL)); // Seed the random number generator

    std::vector<Record *> lists;
    int numberOfLists =
        generateRandomInt(5, 3455); // Generate between 5 and 10 lists

    std::cout << "Generated " << numberOfLists << " random lists:\n";
    int totalElements = 0;
    for (int i = 0; i < numberOfLists; i++) {
        int length = generateRandomInt(1, 2050);
        Record *list = generateRandomList(length);
        totalElements += length;
        lists.push_back(list);
        // printList(lists.back());
    }
    // combine the lists and sort them
    std::vector<Record> combined;
    for (Record *list : lists) {
        Record *current = list;
        while (current) {
            combined.push_back(*current);
            current = current->next;
        }
    }
    sort(combined.begin(), combined.end());
    // print the combined list
    // std::cout << "Combined list: ";
    // for (Record r : combined) {
    //     printf("%s ", r.repr());
    // }

    // Create LoserTree and merge lists
    LoserTree lt;
    Record *result = lt.mergeKLists(lists);

    // Print the merged result
    // std::cout << "Merged list : ";
    // printList(result);

    // Verify that the merged result is correctly sorted
    assert(isSorted(result));
    std::cout << "Verification passed: The merged list is sorted.\n";
    int count = 0;
    Record *current = result;
    while (current) {
        count++;
        current = current->next;
    }
    std::cout << "Total number of elements: " << count << "/" << totalElements
              << std::endl;
    assert(count == totalElements);
    std::cout
        << "Verification passed: The merged list contains the correct number "
           "of elements.\n";

    // match the combined list with the merged list
    current = result;
    for (Record r : combined) {
        int cmp = std::strncmp(r.data, current->data, RECORD_KEY_SIZE);
        if (cmp != 0) {
            printf("Error: Mismatch in merged list: %s != %s\n", r.repr(),
                   current->repr());
        }
        assert(cmp == 0);
        current = current->next;
    }
    std::cout << "Verification passed: The merged list matches the combined "
                 "list.\n";

    return 0;
}

int main() {
    automatedTest();
    exit(0);
}
