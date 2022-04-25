#pragma once

#ifndef IoI_H
#define IoI_H


#include <iostream>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <fstream>
#include "db/IoINode.h"





class IoI {
    public:
        IoI();
        IoI(std::string name);
        void addIndex(std::string path);
        void addIndex(std::string path, leveldb::VersionSet* vsh, leveldb::TableBuilder* tbh, leveldb::FileMetaData* mth);
        void getSDS(std::string path, leveldb::VersionSet** vsh, leveldb::TableBuilder** tbh, leveldb::FileMetaData** mth);
        void getVSH(std::string path, leveldb::VersionSet** vsh);
        void getTBH(std::string path, leveldb::TableBuilder** tbh);
        void setTBH(std::string path, leveldb::TableBuilder* tbh);
        void getMTH(std::string path, leveldb::FileMetaData** mth);
        void removeNode(std::string nodePath);
        IoINode* getSensorNode(std::string path);
        void printIndex();
    private:

        void split(const std::string &s, char delim, std::vector<std::string>* result);
        std::string name;
        char delimiter = ':';
        IoINode* root;
        void createRootNode(std::string name);

};

#endif