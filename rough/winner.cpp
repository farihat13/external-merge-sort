#include <climits>
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
} // gen_a_record

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

class WinnerTree {
  public:
    void constructTree(vector<int> &winnerTree, vector<Record *> &lists,
                       int k) {

        printf("k (#leaves): %d\n", k);
        int n = 2 * k;
        // Set the leaf values using the first elements of the lists
        printf("leaf init: \n");
        for (int i = 0; i < k; i++) {
            printf(" %d:[%d], ", i, n - k + i);
            winnerTree[n - k + i] = lists[i] ? lists[i]->val : INT_MAX;
        }
        printf("\nleaf init done\n");

        // print the winner tree
        printf("winner tree: ");
        for (int i = 1; i < n; i++) {
            printf("[%d]:%d ", i, winnerTree[i]);
        }
        printf("\n");

        // populate the internal nodes with the min of children
        printf("internal init: \n");
        for (int i = n - k - 1; i > 0; i--) {
            printf(" [%d], ", i);
            winnerTree[i] = min(winnerTree[2 * i], winnerTree[2 * i + 1]);
        }
        printf("\ninternal init done\n");

        // print the winner tree
        printf("winner tree: ");
        for (int i = 1; i < n; i++) {
            printf("[%d]:%d ", i, winnerTree[i]);
        }
        printf("\n");
    }

    int getNext(vector<int> &winnerTree, vector<Record *> &lists, int k) {
        int currentWinner = winnerTree[1];
        int n = winnerTree.size();
        int i = 1; // root of the winner tree
        printf("\n\tcurrent winner: %d\n", winnerTree[1]);

        // Find the leaf corresponding to the "winner"
        while (i < n / 2) {
            i = 2 * i + (winnerTree[2 * i] != winnerTree[i]);
            printf("\ti: %d", i);
        }
        printf(" found leaf\n");

        int indexOfList = i - (n / 2);
        int newval;
        printf("\tindexOfList: %d\n", indexOfList);

        // Update the corresponding list
        if (lists[indexOfList]) {
            lists[indexOfList] = lists[indexOfList]->next;
            newval = lists[indexOfList] ? lists[indexOfList]->val : INT_MAX;
        } else {
            newval = INT_MAX;
        }
        printf("\tnewval: %d\n", newval);

        winnerTree[i] = newval;

        // Find the new winner after update
        while (i > 1) {
            i /= 2;
            winnerTree[i] = min(winnerTree[2 * i], winnerTree[2 * i + 1]);
            printf("\t[%d]:%d ", i, winnerTree[i]);
        }
        printf("\tfound new winner\n");

        // print the winner tree
        printf("\twinner tree: ");
        for (int i = 1; i < n; i++) {
            printf("[%d]:%d ", i, winnerTree[i]);
        }
        printf("\n");

        return currentWinner;
    }

    Record *mergeKLists(vector<Record *> &lists) {
        int k = lists.size();
        if (k == 0)
            return NULL;

        vector<int> winnerTree(2 * k, INT_MAX);
        constructTree(winnerTree, lists, k);

        Record *head = NULL, *temp = NULL;

        // Keep taking out the winner till all values are gone
        while (true) {
            int toput = getNext(winnerTree, lists, k);
            if (toput == INT_MAX)
                break;
            printf("toput: %d\n", toput);

            Record *newNode = new Record(toput);
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
    // Example usage: merging 3 sorted lists
    Record *list1 = new Record(1);
    list1->next = new Record(4);
    list1->next->next = new Record(5);

    Record *list2 = new Record(100);
    list2->next = new Record(300);
    list2->next->next = new Record(400);

    Record *list3 = new Record(200);
    list3->next = new Record(600);

    vector<Record *> lists;
    lists.push_back(list1);
    lists.push_back(list2);
    lists.push_back(list3);
    // for (int i = 0; i < 3; i++) {
    //     Record *list = new Record(i + 1);
    //     list->next = new Record(i + 10);
    //     list->next->next = new Record(i + 20);
    //     list->next->next->next = new Record(i + 30);
    //     lists.push_back(list);
    // }
    for (Record *list : lists) {
        printList(list);
    }
    WinnerTree tt;
    Record *result = tt.mergeKLists(lists);

    cout << "Merged list: ";
    printList(result);

    return 0;
}
