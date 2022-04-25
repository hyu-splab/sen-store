#pragma once

#ifndef IoI_NODE_H
#define IoI_NODE_H


// Created by Kihan Choi
#include<iostream>
#include<unordered_map>
#include<db/version_set.h>
#include<leveldb/table_builder.h>
#include<db/version_edit.h>

class VersionSet;
class TableBuilder;
class FileMetaData;

class IoINode {

public:
    IoINode() {}
    IoINode(std::string name);
    ~IoINode();
    std::string getName();
    void addChild(std::string name);
    void deleteChild(std::string name);
    IoINode* getChild(std::string name);
    void deleteAllChildNode();
    void print(int depth);
    void initSDS();
    leveldb::VersionSet* vsh;
    leveldb::TableBuilder* tbh;
    leveldb::FileMetaData* mth;

private:
    std::string name;
    //IoINode<K, V> **forward;
    int nodeLevel;
    std::unordered_map<std::string, IoINode*> children;

};


#endif //IoI_NODE_H
