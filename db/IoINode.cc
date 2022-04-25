#include "db/IoINode.h"
void IoINode::print(int depth = 0) {

    for (int i=0; i<depth; ++i) {
        if (i!=depth-1) //last element
            std::cout << "     ";
        else
            std::cout << "|-- ";
    }
    std::cout << this->name << std::endl;

    for (std::pair<std::string, IoINode*> node : this->children) {
        node.second->print(depth+1);
    }

}

void IoINode::initSDS() {

}

void IoINode::addChild(std::string name) {
    if (children.count(name) == 0) {
        IoINode* child = new IoINode(name);
        children[name] = child;
    }
};
void IoINode::deleteChild(std::string name) {
    if (children.count(name) != 0) {
        children.erase(name);
    }
}

IoINode* IoINode::getChild(std::string name) {
    std::unordered_map<std::string, IoINode*>::const_iterator got = children.find(name);
    if (got == children.end()) {
        std::cout << "getChild failed (Not found)" << std::endl;
        return NULL;
    }
    else {
        return got->second;
    }
}

void IoINode::deleteAllChildNode() {
    int nodeNum = children.size();
    if (nodeNum != 0) {
        auto it = children.begin();
        while (it != children.end()) {
            it->second->deleteAllChildNode();
            it = children.erase(it);
        }
        std::cout << "[Sen-Store] Total " << nodeNum << " Sub Node(s) Removed\n";
    } else { //leaf node
        delete vsh;
        delete tbh;
        delete mth;
    }
}


IoINode::IoINode(std::string name) {
    this->name = name;
};

IoINode::~IoINode() {
    //delete[]forward;
};

std::string IoINode::getName() {
    return name;
}

