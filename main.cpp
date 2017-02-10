/*-------------------------------------------------------
 *
 *  g++ -std=c++11 -o main main.cpp btnode.cpp btree.cpp
 *
 * ------------------------------------------------------*/
#include <iostream>
#include <chrono>
#include <errno.h>
#include <unistd.h>
#include <getopt.h>
#include <string>

#include "btnode.h"
#include "btree.h"

#include "bptnode.h"
#include "bptree.h"
#include "vlinklist.hpp"

#include "boost_logger.h"

const char* dbfile = "log.txt";
size_t dbsize = 1024*1024*1024;

//Supported Data-Structures
typedef enum {
    LINKLIST,
    BTREE,
    BPTREE
}PERSISTENCE_FORMAT;

struct options {
   PERSISTENCE_FORMAT type;
   bool create;
   bool clear;
   bool add;
   bool erase;
   int key;
   bool print;
};

int
parse(int argc, char **argv, struct options* opt) {
	int c = 0;
        int opt_index = 0;

        static struct option long_options[] = {

             //Features we support

            {"type", required_argument, 0, 't'},

            {"create", optional_argument, 0, 'c'},

            {"delete", optional_argument, 0, 'd'},

            {"add", optional_argument, 0, 'a'},

            {"remove", optional_argument, 0, 'r'},

            {"print", optional_argument, 0, 'p'}
        };

	while ((c = getopt_long(argc, argv, "t:a:rcdp",
                        long_options, &opt_index)) != -1) {

       	    switch(c) {
       	    case 't': {
                    std::vector<std::string> v{"list", "bptree", "btree"};
                    auto it = std::find(v.begin(), v.end(), optarg);
                    if (it == v.end())
                        return -1;
                    opt->type = static_cast<PERSISTENCE_FORMAT>(it - v.begin());
                    break;
                }
       	    case 'c':
                opt->create = true;
                break;
       	    case 'd':
                opt->clear = true;
                break;
       	    case 'a':
                opt->add = true;
                if (optarg)
                   opt->key = atoi(optarg);
                else
                   return -EINVAL;
                break;
       	    case 'r':
                opt->erase = true;
                break;
       	    case 'p':
                opt->print = true;
                break;
            default:
                cerr << "Usage : [--create] [--type] type" << endl;
                return -1;
            }
        }
        return 0;
}

int main(int argc, char **argv) {

	struct options opt;
        bzero((char*)&opt, sizeof(opt));

        if (argc < 3) {
                cerr << "Usage : [--type] type [--create] [--delete] [-add] [--remove] [--print]" << endl;
		return -EINVAL;
        }

	if (parse(argc, argv, &opt) < 0)
		return -EINVAL;

        // Set-Up Boost Logging
        init_boost_logger();

        BOOST_LOG_TRIVIAL(info)
            << " --type " << opt.type
            << " --create " << opt.create
            << " --delete " << opt.clear
            << " --add " << opt.add
            << " --remove " << opt.erase
            << " --key " << opt.key
            << " --print " << opt.print;

        // Init Storage Media
        StorageResource sink(std::string(dbfile), dbsize);
        boost::shared_ptr<CoreIO> io = boost::shared_ptr<CoreIO>(new CoreIO(sink));
        boost::shared_ptr<StorageAllocator> allocator =
            boost::shared_ptr<StorageAllocator>(new StorageAllocator(sink, 0, sink.size()));

        if (LINKLIST == opt.type) {
           typedef PersistentLinkList<int, CoreIO, StorageAllocator> PMemLinkList;
           boost::shared_ptr<PMemLinkList> pList;

           //Create Link List or get Handle
           pList = boost::shared_ptr<PMemLinkList>(new PMemLinkList(io, allocator));

           if (opt.clear)
               pList->clear();

           if (opt.add)
               pList->push_back(opt.key);

           if (opt.erase)
               pList->pop_back();

           if (opt.print)
               pList->print();
        }

        return 0;
}
