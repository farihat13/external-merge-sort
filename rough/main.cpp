#include <iostream>
#include <vector>
#include <limits>

class LoserTree {
private:
    std::vector<int> loserTree;
    std::vector<int> internalNodes;
    std::vector<int> inputArray;
    int k;

    void buildLoserTree() {
        // Initialize internal nodes with -1 (indicating no loser yet)
        std::fill(internalNodes.begin(), internalNodes.end(), -1);

        // Compare elements in pairs and fill the internalNodes array with losers
        for (int i = 0; i < k; ++i) {
            printf("\nIteration %d\n", i);
            int winner = i;
            printf("winner: [%d]%d\n", winner, inputArray[winner]);
            for (int j = 0; j < k; ++j) {
                printf("j: %d (%d)\n", j, internalNodes[j]);
                if (internalNodes[j] == -1) {
                    internalNodes[j] = winner;
                    printf("\tinternalNodes[%d]: [%d]%d\n", j, winner, inputArray[winner]);
                    break;
                } else {
                    int loser = (inputArray[winner] > inputArray[internalNodes[j]]) ? winner : internalNodes[j];
                    printf("loser: [%d]%d\n", loser, inputArray[loser]);
                    winner = (loser == winner) ? internalNodes[j] : winner;
                    printf("winner: [%d]%d\n", winner, inputArray[winner]);
                    internalNodes[j] = loser;
                    printf("internalNodes[%d]: [%d]%d\n", j, loser, inputArray[loser]);
                }
            }
        }
        printf("\n");

        // print internal nodes
        for (int i = 0; i < 2 * k; i++) {
            printf("%d ", internalNodes[i]);
        }


        // // Copy the final losers to the loserTree array
        // std::copy(internalNodes.begin(), internalNodes.end(), loserTree.begin());
    }

public:
    LoserTree(const std::vector<int>& inputArray)
        : inputArray(inputArray), k(inputArray.size()), loserTree(k), internalNodes(k) {
        internalNodes.resize(2 * k, std::numeric_limits<int>::max());
        buildLoserTree();
    }

    const std::vector<int>& getLoserTree() const {
        return loserTree;
    }
};

int main() {
    std::vector<int> inputArray = {4, 3, 6, 8, 1, 5, 7, 3};
    LoserTree loserTree(inputArray);

    const std::vector<int>& tree = loserTree.getLoserTree();
    std::cout << "Loser tree indices (representing input array indices): ";
    for (int index : tree) {
        std::cout << index << " ";
    }
    std::cout << std::endl;

    return 0;
}
