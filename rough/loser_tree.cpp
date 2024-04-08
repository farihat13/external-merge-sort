#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <vector>

class Node {
  public:
    int index;
    int value;
    Node() {
        index = -1;
        value = -1;
    }
};

class LoserTree {
  public:
    int *inputArray;
    Node *loserTree;
    int size;
    LoserTree(int *inputArray, int k) {
        this->loserTree = new Node[k];
        this->inputArray = inputArray;
        this->size = k;
        buildLoserTree();
        printLoserTree();
    }
    int parent(int i) {
        return i / 2;
    }
    int leftChild(int i) {
        return 2 * i;
    }
    int rightChild(int i) {
        return 2 * i + 1;
    }
    void buildLoserTree() {
        int* curr = inputArray;
        
    }
    void printLoserTree() {
        printf("Loser Tree:\nwinner: %d\n", loserTree[0].value);
        int row = 1;
        for (int i = 1; i < size;) {
            for (int j = 0; j < row; j++) {
                printf("%d ", loserTree[i++].value);
            }
            row *= 2;
            printf("\n");
        }
        printf("\n");
    }
};

int naive_loser_tree_step_by_step() {
    int n = 8;
    int arr[] = {4, 3, 6, 8, 1, 5, 7, 3};
    int loser[8];
    int winner[8];
    printf("loser: ");
    for (int i = 0; i < 8; i += 2) {
        loser[i / 2] = arr[i];
        winner[i / 2] = arr[i + 1];
        if (arr[i] < arr[i + 1]) {
            loser[i / 2] = arr[i + 1];
            winner[i / 2] = arr[i];
        }
        printf("%d ", loser[i / 2]);
    }
    printf("\n");
    printf("winner: ");
    for (int i = 0; i < 4; i++) {
        printf("%d ", winner[i]);
    }
    printf("\n");
    for (int i = 0; i < 4; i++) {
        arr[i] = winner[i];
    }

    printf("loser:");
    for (int i = 0; i < 4; i += 2) {
        loser[i / 2] = arr[i];
        winner[i / 2] = arr[i + 1];
        if (arr[i] < arr[i + 1]) {
            loser[i / 2] = arr[i + 1];
            winner[i / 2] = arr[i];
        }
        printf("%d ", loser[i / 2]);
    }
    printf("\n");
    printf("winner: ");
    for (int i = 0; i < 2; i++) {
        printf("%d ", winner[i]);
    }
    printf("\n");
    for (int i = 0; i < 2; i++) {
        arr[i] = winner[i];
    }

    printf("loser:");
    for (int i = 0; i < 2; i += 2) {
        loser[i / 2] = arr[i];
        winner[i / 2] = arr[i + 1];
        if (arr[i] < arr[i + 1]) {
            loser[i / 2] = arr[i + 1];
            winner[i / 2] = arr[i];
        }
        printf("%d ", loser[i / 2]);
    }
    printf("\n");
    printf("winner: ");
    for (int i = 0; i < 1; i++) {
        printf("%d ", winner[i]);
    }
    printf("\n");
    for (int i = 0; i < 1; i++) {
        arr[i] = winner[i];
    }
    return 0;
}

int main() {
    LoserTree loserTree = LoserTree(new int[8]{4, 3, 6, 8, 1, 5, 7, 3}, 8);
}