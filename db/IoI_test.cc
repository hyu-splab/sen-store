#include <iostream>
#include "IoINode.h"
#include "IoI.h"
#include <vector>
#include <sstream>
#include <unordered_map>

void split(const std::string &s, char delim, std::vector<std::string>* result) {
    std::istringstream iss(s);
    std::string item;

    while (std::getline(iss, item, delim)) {
        result->push_back(item);
    }
}


int main(){
    std::cout << "oooo IoI TEST FILE oooo" << std::endl;
    std::string testPath, testPath2, testPath3;
    char delimiter = ':';
    std::string token;
    testPath = "region1:factory1:device1:sensor1";
    testPath2 = "region1:factory2:device1:sensor1";
    testPath3 = "region1:factory2:device2:sensor1";


    std::vector<std::string> pathIter;
   
   IoI* index = new IoI("test");
   index->addIndex(testPath2);
   index->addIndex(testPath);
   index->addIndex(testPath3);
   index->printIndex();
   

    return 0;
}