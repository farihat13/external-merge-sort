#include <climits>
#include <cstring>
#include <iostream>
#include <vector>

using namespace std;

#define RECORD_SIZE 4
#define RECORD_KEY_SIZE 2

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
  public:
    void constructTree(vector<int> &loserTree, vector<Record *> &lists, int k) {
        int n = 2 * k;

        // Initialize leaves with the first elements
        for (int i = 0; i < k; i++) {
            loserTree[n - k + i] = lists[i] ? lists[i]->val : INT_MAX;
        }
        printf("leaf init: ");
        for (int i = 0; i < n; i++) {
            printf("[%d]:%d ", i, loserTree[i]);
        }
        printf("\n");

        // Construct the loser tree from leaves up
        int x = 0;
        for (int i = n - k - 1; i > 0; i--) {
            if (x == k / 2) {
                break;
            }
            x++;
            int left = 2 * i;
            int right = left + 1;
            int loserIdx = loserTree[left] < loserTree[right] ? right : left;
            int loser = loserTree[loserIdx];
            int winnerIdx = loserIdx == left ? right : left;
            int winner = loserTree[winnerIdx];
            loserTree[i] = loser;
            printf("\ti:[%d]:%d, loser: [%d]:%d, winner: [%d]:%d\n", i, loser,
                   loserIdx, loser, winnerIdx, winner);
            int parent = i;
            while (parent >= 0) {
                parent /= 2;
                printf("\t\tparent: [%d]:%d -> \n", parent, loserTree[parent]);
                if (loserTree[parent] == INT_MAX) {
                    loserTree[parent] = winner;
                    printf("\t\t\tn: %d\n", loserTree[parent]);
                    break;
                }
                if (winner > loserTree[parent]) {
                    printf("\t\t\tn: %d, w %d\n", loserTree[parent], winner);
                    std::swap(loserTree[parent], winner);
                }
                if (parent == 0) {
                    break;
                }
            }
            printf("\t\tloser tree: ");
            for (int i = 0; i < n; i++) {
                printf("[%d]:%d ", i, loserTree[i]);
            }
            printf("\n");
        }

        printf("loser tree: ");
        for (int i = 0; i < n; i++) {
            printf("[%d]:%d ", i, loserTree[i]);
        }
        printf("\n");
    }

    int getNext(vector<int> &loserTree, vector<Record *> &lists, int k) {
        int currWinner = loserTree[0];
        int n = loserTree.size();

        // Trace down the tree from the root to find the winning leaf;
        // TODO: store ptr in the tree
        int winningIdx = 1;
        for (int i = k; i < 2 * k; i++) {
            if (loserTree[i] == currWinner) {
                winningIdx = i;
                break;
            }
        }
        printf("\n\twinner: %d, winningIdx: %d\n", currWinner, winningIdx);

        // Update the tree with the next value from the list corresponding to
        // the found leaf
        int indexOfList = winningIdx - (n / 2);
        int newval;
        if (lists[indexOfList]) {
            lists[indexOfList] = lists[indexOfList]->next;
            newval = lists[indexOfList] ? lists[indexOfList]->val : INT_MAX;
        } else {
            newval = INT_MAX;
        }
        loserTree[winningIdx] = newval;


        // Reconstruct the path from the leaf to the root
        int parent = winningIdx;
        int winner = newval;
        while (parent >= 0) {
            parent /= 2;
            printf("\t\tparent: [%d]:%d -> \n", parent, loserTree[parent]);
            if (loserTree[parent] == INT_MAX) {
                loserTree[parent] = winner;
                printf("\t\t\tn: %d\n", loserTree[parent]);
                break;
            }
            if (winner > loserTree[parent]) {
                printf("\t\t\tn: %d, w %d\n", loserTree[parent], winner);
                std::swap(loserTree[parent], winner);
            }
            if (parent == 0) {
                break;
            }
        }

        return currWinner;
    }

    Record *mergeKLists(vector<Record *> &lists) {
        int k = lists.size();
        if (k == 0)
            return NULL;

        vector<int> loserTree(2 * k, INT_MAX);
        constructTree(loserTree, lists, k);

        Record *head = NULL, *temp = NULL;

        while (true) {
            int minElement = getNext(loserTree, lists, k);
            if (minElement == INT_MAX) {
                break;
            }

            Record *newNode = new Record(minElement);
            if (!head) {
                head = newNode;
            } else {
                temp->next = newNode;
            }
            temp = newNode;
        }

        return head;
    }
};

// Helper function to print the linked list
void printList(Record *node) {
    while (node) {
        cout << node->val << " ";
        node = node->next;
    }
    cout << endl;
}

int main() {
    int arr[] = {4, 3, 6, 8, 1, 5, 7, 3};
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
    for (int i = 0; i < 5; i++) {
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
    Record *result = lt.mergeKLists(lists);

    cout << "Merged list: ";
    printList(result);

    return 0;
}
