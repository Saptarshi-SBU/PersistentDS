/*-----------------------------------------------------------------------------
 *
 *  Copyright(C): 2017
 *
 *  Registry information of Persistent Data Structures
 *
 * ----------------------------------------------------------------------------*/
#include <list>

#include <boost/functional/hash.hpp>

#include "storage_allocator.hpp"
#include "meta.pb.h"

using namespace std;

#define ROUNDUP(pos, align) (pos + (pos % align))
#define REGISTRY_SIGNATURE (0xdeadbeef)
template <class IO, class Allocator>
class Registry {

    private:

       size_t region_size;

       // headers which are stamped per record
       uint64_t magic;
       size_t write_size;

       // registry entries always start at this alignment
       const size_t alignment = 128;

       // bump allocator
       off_t cursor;

       // record list
       std::list<db::registryrecord> reg_list;

       boost::shared_ptr<IO> _core;

       boost::shared_ptr<Allocator> _allocator;

    public:

       Registry(size_t size,
          boost::shared_ptr<IO> core, boost::shared_ptr<Allocator> alloc) :
          region_size(size), _core(core), _allocator(alloc) {

          // Search for the Signature
          for (off_t pos = 0; (pos < region_size); pos+=alignment) {
             _core->Read(pos, (char*)&magic, sizeof(magic));
              if (magic != REGISTRY_SIGNATURE)
                  continue;
              pos+=sizeof(magic);
             _core->Read(pos, (char*)&write_size, sizeof(write_size));
              auto rbuf = new char[write_size];
              db::registryrecord rec;
              pos+=sizeof(write_size);
             _core->Read(pos, rbuf, write_size);
              rec.ParseFromString(std::string(rbuf));
              pos+=write_size;
              delete rbuf;
              reg_list.push_back(rec);
              cursor = ROUNDUP(pos, alignment);
          }

          if (reg_list.empty()) {
              auto ans = _allocator->Allocate(region_size);
              cursor = ROUNDUP(ans.first, alignment);
          }
       }

       ~Registry() {
           reg_list.clear();
       }

       bool find(size_t id, db::registryrecord& rec) {
           for (auto &i : reg_list) {
               if (i.key() == id) {
                   rec = i;
                   return true;
               }
           }
           return false;
       }

       // Note we have already reserved region from the
       // allocator. We use the cursor based bump allocator method
       // to allocate entries from this region
       void insert(size_t id) {

           std::string str;
           db::registryrecord rec;
           rec.set_key(id);
           assert(cursor % alignment == 0);
           rec.set_phys_curr(cursor);
           rec.set_write_gen(cursor);
           rec.SerializeToString(&str);
           auto pos = cursor;
          _core->Write(pos, (char*)&magic, sizeof(magic));
           pos+=sizeof(magic);
           write_size = str.size();
          _core->Write(pos, (char*)&write_size, sizeof(write_size));
           pos+=sizeof(write_size);
          _core->Write(pos, str.c_str(), write_size);
           cursor+=alignment;
       }

       void update(db::registryrecord& rec) {
           if (reg_list.empty())
               return;

           auto iter = reg_list.begin();
           for (;iter!=reg_list.end();iter++)
               if ((*iter).key() == rec.key())
                   break;
#if 0
           auto iter = std::find(reg_list.begin(), reg_list.end(),
                   [rec] (const db::registryrecord& rc) {
                   return (rc.key() == rec.key());});
#endif

           if (iter == reg_list.end())
               return;

           *iter = rec;
           auto pos = (*iter).phys_curr();
           pos+=sizeof(magic);

           //Update only size and record
           std::string str;
           (*iter).SerializeToString(&str);
           write_size = str.size();
          _core->Write(pos, (char*)&(write_size), sizeof(write_size));
           pos+=sizeof(write_size);
          _core->Write(pos, str.c_str(), write_size);
       }

       void remove(size_t id) {

           if (reg_list.empty())
               return;

           auto iter = reg_list.begin();
           for (;iter!=reg_list.end();iter++)
               if ((*iter).key() == id)
                   break;
#if 0
           auto iter = std::find(reg_list.begin(), reg_list.end(),
                   [id] (const db::registryrecord& rec) {
                   return (rec.key() == id);});
#endif

           if (iter == reg_list.end())
               return;

           std::string str;
           (*iter).set_phys_next(0);
           (*iter).set_nr_elements(0);
           (*iter).set_write_gen(-1);
           (*iter).SerializeToString(&str);
           auto pos = (*iter).phys_curr();
           pos+=sizeof(magic);

           //Update only size and record
           write_size = str.size();
          _core->Write(pos, (char*)&(write_size), sizeof(write_size));
           pos+=sizeof(write_size);
          _core->Write(pos, str.c_str(), write_size);

           //remove from list
           reg_list.erase(iter);
      }
};
