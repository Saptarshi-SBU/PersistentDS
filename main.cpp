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

db::registryrecord::PersistenceType
GetIndex(char *str) {
   const vector<string> v{"LIST", "BTREE", "B+TREE"};
   auto it = std::find(v.begin(), v.end(), optarg);
   return (it != v.end()) ?
      static_cast<db::registryrecord::PersistenceType>(it - v.begin()) :
      db::registryrecord::NSUP;
}

template<class IO, class Allocator>
db::registryrecord::PersistenceType
GetType(size_t key, boost::shared_ptr<Registry<IO, Allocator>> reg) {
   db::registryrecord rec;
   reg->find(key, rec);
   return rec.type();
}

template<class IO, class Allocator>
bool isSnapshot(size_t key, boost::shared_ptr<Registry<IO, Allocator>> reg) {
   db::registryrecord rec;
   auto ret = reg->find(key, rec);
   if (ret)
       return rec.issnap();
}

template<class IO, class Allocator>
bool isvalidkey(size_t key, boost::shared_ptr<Registry<IO, Allocator>> reg) {
   db::registryrecord rec;
   return reg->find(key, rec);
}

struct options {
   db::registryrecord::PersistenceType type;
   bool create;
   bool clear;
   bool add;
   bool erase;
   int key;
   char *id;
   char *parent;
   bool print;
   bool snapshot;
};

int
parse(int argc, char **argv, struct options* opt) {
	int c = 0;
        int opt_index = 0;

        static struct option long_options[] = {

             //Features we support

            {"type", required_argument, 0, 't'},
            {"create", required_argument, 0, 'c'},
            {"delete", required_argument, 0, 'd'},
            {"add", required_argument, 0, 'a'},
            {"remove", no_argument, 0, 'r'},
            {"print", required_argument, 0, 'p'},
            {"snapshot", required_argument, 0, 's'},
            {"id", required_argument, 0, 'i'}
        };

	while ((c = getopt_long(argc, argv, "t:c:d:a:rp:s:i:",
                        long_options, &opt_index)) != -1) {

       	    switch(c) {
       	    case 't':
                opt->type = GetIndex(optarg);
                break;
       	    case 'c':
                opt->id = optarg;
                opt->create = true;
                break;
       	    case 'd':
                opt->id = optarg;
                opt->clear = true;
                break;
       	    case 'a':
                opt->key = atoi(optarg);
                opt->add = true;
                break;
       	    case 'r':
                // erase is only pop_back
                opt->erase = true;
                break;
       	    case 'p':
                opt->id = optarg;
                opt->print = true;
                break;
       	    case 's':
                opt->parent = optarg;
                opt->snapshot = true;
                break;
       	    case 'i':
                opt->id = optarg;
                break;
            default:
                cerr << "Usage : [--create] [--type] type" << endl;
                return -1;
            }
        }

        if (opt->type == db::registryrecord::NSUP) {
           cout << "Data-structure not supported/invalid " << endl;
	   return -ENOTSUP;
        }

        if (opt->id == nullptr) {
           cout << "Version name not specified " << endl;
	   return -EINVAL;
        }

        return 0;
}

int main(int argc, char **argv) {
	struct options opt;

        // Note: in case we have string as member
        // bzero will corrupt the string object
        bzero((char*)&opt, sizeof(opt));

        if (argc < 3) {
           cerr << "Usage : [--type] type [--create] [--delete] [-add] [--remove] [--print]" << endl;
	   return -EINVAL;
        }

	if (parse(argc, argv, &opt) < 0)
	   return -EINVAL;

        init_boost_logger();

        BOOST_LOG_TRIVIAL(debug)
            << " --type " << opt.type
            << " --create " << opt.create
            << " --id " << opt.id
            << " --delete " << opt.clear
            << " --add " << opt.add
            << " --remove " << opt.erase
            << " --key " << opt.key
            << " --print " << opt.print
            << " --snapshot " << opt.snapshot;

        // Init Storage Media
        StorageResource sink(std::string(dbfile), dbsize);
        boost::shared_ptr<CoreIO> io = boost::shared_ptr<CoreIO>(new CoreIO(sink));
        boost::shared_ptr<StorageAllocator> allocator =
            boost::shared_ptr<StorageAllocator>(new StorageAllocator(sink, 0, sink.size()));

        // Init Registry
        typedef Registry<CoreIO, StorageAllocator> GlobalReg;
        auto reg = boost::shared_ptr<GlobalReg>(new GlobalReg(MAX_READ, io, allocator));

        size_t key, parent_key;

        if (opt.id)
           key = boost::hash_value(string(opt.id));

        if (opt.parent)
           parent_key = boost::hash_value(string(opt.parent));

        if (opt.snapshot && !isvalidkey(parent_key, reg)) {
           BOOST_LOG_TRIVIAL(error) << "Parent Entry not found! " << parent_key;
           return -ENOENT;
        }  else if (!opt.snapshot && !opt.create && !isvalidkey(key, reg)) {
           BOOST_LOG_TRIVIAL(error) << "Entry not found! " << key;
           return -ENOENT;
        }

        auto type = opt.create ? opt.type : GetType(key, reg);

        bool snap = opt.create ? false : isSnapshot(key, reg);

        switch (type) {
        case db::registryrecord::LIST: {

           typedef PersistentLinkList<int, CoreIO, StorageAllocator> PMemLinkList;
           boost::shared_ptr<PMemLinkList> pList;

           if (opt.snapshot)
              reg->snapshot(key, parent_key);
           else
              pList = boost::shared_ptr<PMemLinkList>
                 (new PMemLinkList(string(opt.id), reg, io, allocator));

           if (opt.clear && pList && !snap)
               pList->clear();

           if (opt.clear && snap)
               reg->remove(key);

           if (opt.add && pList && !snap)
               pList->push_back(opt.key);

           if (opt.erase && pList && !snap)
               pList->pop_back();

           if (opt.print && pList)
               pList->print();

           break;
        }

        default:
           BOOST_LOG_TRIVIAL(info) << "Unsupported!";
           break;
        }

        return 0;
}
