#include "db/IoI.h"

IoI::IoI() {
    createRootNode("senstore");
}
IoI::IoI(std::string name) {
    this->name = name;
    createRootNode(name);
}
void IoI::createRootNode(std::string name) {
    this->name = name;
    this->root = new IoINode("root");
    std::cout << "##### SEN-STORE IoI CREATED #####" << std::endl;
}
void IoI::printIndex() {
    this->root->print(0);
}

void IoI::removeNode(std::string nodePath) {
        std::vector<std::string> pathIter;
        std::cout << "[Sen-Store] RemoveNode(" << nodePath << ") called\n";
        this->split(nodePath, delimiter, &pathIter);

        IoINode* current = this->root;
        IoINode* prev = current;

        for(auto m : pathIter) {
            prev = current;
            current = current->getChild(m);    
        }
        current->deleteAllChildNode();
        prev->deleteChild(current->getName());
        //printIndex();
    
}


void IoI::addIndex(std::string path) {
        std::vector<std::string> pathIter;
        this->split(path, delimiter, &pathIter);

        IoINode* current = this->root;
        for(auto m : pathIter) {
            //std::cout << m << std::endl;
            current->addChild(m);
            current = current->getChild(m);    
        }
        std::cout << "[Sen-Store] Sensor " << path << " added\n";
        //root->print();
};

void IoI::addIndex(std::string path, leveldb::VersionSet* vsh, leveldb::TableBuilder* tbh, leveldb::FileMetaData* mth) {
        std::vector<std::string> pathIter;
        this->split(path, delimiter, &pathIter);

        IoINode* current = this->root;
        for(auto m : pathIter) {
            //std::cout << m << std::endl;
            current->addChild(m);
            current = current->getChild(m);    
        }
        current->vsh = vsh;
        current->tbh = tbh;
        current->mth = mth;

        //for debugging
        //root->print(0);
};
IoINode* IoI::getSensorNode(std::string path) {
        std::vector<std::string> pathIter;
        this->split(path, delimiter, &pathIter);
        IoINode* current = this->root;

        for(auto m : pathIter) {
            current = current->getChild(m);    
        }
        return current;
}

void IoI::getSDS(std::string path, leveldb::VersionSet** vsh, leveldb::TableBuilder** tbh, leveldb::FileMetaData** mth) {
        IoINode* current = getSensorNode(path);
        *vsh = current->vsh;
        *tbh = current->tbh;
        *mth = current->mth;
}
void IoI::getTBH(std::string path, leveldb::TableBuilder** tbh) {
        IoINode* current = getSensorNode(path);
        *tbh = current->tbh;

}
void IoI::setTBH(std::string path, leveldb::TableBuilder* tbh) {
        IoINode* current = getSensorNode(path);
        current->tbh = tbh;
}

void IoI::getVSH(std::string path, leveldb::VersionSet** vsh) {
        IoINode* current = getSensorNode(path);
        *vsh = current->vsh;
}
void IoI::getMTH(std::string path, leveldb::FileMetaData** mth) {
        IoINode* current = getSensorNode(path);
        *mth = current->mth;
}

void IoI::split(const std::string &s, char delim, std::vector<std::string>* result) {
    std::istringstream iss(s);
    std::string item;

    while (std::getline(iss, item, delim)) {
        result->push_back(item);
    }
};