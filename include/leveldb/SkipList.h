#ifndef SkipListMapPRO_SkipListMap_H
#define SkipListMapPRO_SkipListMap_H

#include <cstddef>
#include <cassert>
#include <ctime>
#include "Node.h"
#include "random.h"
#include <iostream>

#define DEBUG

using namespace std;
template<typename K, typename V>
class SkipListMap {

public:
    SkipListMap(K footerKey) : rnd(0x12345678) {
        createList(footerKey);
    }

    ~SkipListMap() {
        freeList();
    }

    Node<K, V> *search(K key) const;

    bool insert(K key, V value);

    bool remove(K key, V &value);

    int size() {
        return nodeCount;
    }

    int getLevel() {
        return level;
    }

private:
    void createList(K footerKey);

    void freeList();

    void createNode(int level, Node<K, V> *&node);

    void createNode(int level, Node<K, V> *&node, K key, V value);

    int getRandomLevel();

    void dumpAllNodes();

    void dumpNodeDetail(Node<K, V> *node, int nodeLevel);

private:
    int level;
    Node<K, V> *header;
    Node<K, V> *footer;

    size_t nodeCount;

    static const int MAX_LEVEL = 16;

    Random rnd;
};

template<typename K, typename V>
void SkipListMap<K, V>::createList(K footerKey) {
    createNode(0, footer);

    footer->key = footerKey;
    this->level = 0;
    createNode(MAX_LEVEL, header);
    for (int i = 0; i < MAX_LEVEL; ++i) {
        header->forward[i] = footer;
    }
    nodeCount = 0;
}

template<typename K, typename V>
void SkipListMap<K, V>::createNode(int level, Node<K, V> *&node) {
    node = new Node<K, V>(NULL, NULL);
    node->forward = new Node<K, V> *[level + 1];
    node->nodeLevel = level;
    assert(node != NULL);
};

template<typename K, typename V>
void SkipListMap<K, V>::createNode(int level, Node<K, V> *&node, K key, V value) {
    node = new Node<K, V>(key, value);
    if (level > 0) {
        node->forward = new Node<K, V> *[level + 1];
    }
    node->nodeLevel = level;
    assert(node != NULL);
};

template<typename K, typename V>
void SkipListMap<K, V>::freeList() {

    Node<K, V> *p = header;
    Node<K, V> *q;
    //while (p != NULL) {
        //q = p->forward[0];
        //delete p;
    //    p = q;
    //}
    //delete p;
}

template<typename K, typename V>
Node<K, V> *SkipListMap<K, V>::search(const K key) const {
    Node<K, V> *node = header;
    for (int i = level; i >= 0; --i) {
        while ((node->forward[i])->key < key) {
            node = *(node->forward + i);
        }
    }
    node = node->forward[0];
    if (node->key == key) {
        return node;
    } else {
        return node;
    }
};

template<typename K, typename V>
bool SkipListMap<K, V>::insert(K key, V value) {
    Node<K, V> *update[MAX_LEVEL];

    Node<K, V> *node = header;

    for (int i = level; i >= 0; --i) {
        while ((node->forward[i])->key < key) {
            node = node->forward[i];
        }
        update[i] = node;
    }
    node = node->forward[0];

    if (node->key == key) {
        return false;
    }

    int nodeLevel = getRandomLevel();

    if (nodeLevel > level) {
        nodeLevel = ++level;
        update[nodeLevel] = header;
    }

    Node<K, V> *newNode;
    createNode(nodeLevel, newNode, key, value);

    for (int i = nodeLevel; i >= 0; --i) {
        node = update[i];
        newNode->forward[i] = node->forward[i];
        node->forward[i] = newNode;
    }
    ++nodeCount;


    return true;
};

template<typename K, typename V>
void SkipListMap<K, V>::dumpAllNodes() {
    Node<K, V> *tmp = header;
    while (tmp->forward[0] != footer) {
        tmp = tmp->forward[0];
        dumpNodeDetail(tmp, tmp->nodeLevel);
        cout << "----------------------------" << endl;
    }
    cout << endl;
}

template<typename K, typename V>
void SkipListMap<K, V>::dumpNodeDetail(Node<K, V> *node, int nodeLevel) {
    if (node == nullptr) {
        return;
    }
    cout << "node->key:" << node->key << ",node->value:" << node->value << endl;
    for (int i = 0; i <= nodeLevel; ++i) {
        cout << "forward[" << i << "]:" << "key:" << node->forward[i]->key << ",value:" << node->forward[i]->value
             << endl;
    }
}

template<typename K, typename V>
bool SkipListMap<K, V>::remove(K key, V &value) {
    Node<K, V> *update[MAX_LEVEL];
    Node<K, V> *node = header;
    for (int i = level; i >= 0; --i) {
        while ((node->forward[i])->key < key) {
            node = node->forward[i];
        }
        update[i] = node;
    }
    node = node->forward[0];
    if (node->key != key) {
        return false;
    }

    value = node->value;
    for (int i = 0; i <= level; ++i) {
        if (update[i]->forward[i] != node) {
            break;
        }
        update[i]->forward[i] = node->forward[i];
    }

    delete node;

    while (level > 0 && header->forward[level] == footer) {
        --level;
    }

    --nodeCount;

#ifdef DEBUG
    dumpAllNodes();
#endif

    return true;
};

template<typename K, typename V>
int SkipListMap<K, V>::getRandomLevel() {
    int level = static_cast<int>(rnd.Uniform(MAX_LEVEL));
    if (level == 0) {
        level = 1;
    }
    return level;
}

#endif //SkipListMapPRO_SkipListMap_H
