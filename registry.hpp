/*-----------------------------------------------------------------------------
 *
 *  Copyright(C): 2017
 *
 *  Registry debugrmation of Persistent Data Structures
 *
 * ----------------------------------------------------------------------------*/
#include <list>
#include <algorithm>

#include <boost/functional/hash.hpp>

#include "storage_allocator.hpp"
#include "meta.pb.h"

using namespace std;

#define ROUNDUP(pos, align) (pos + align - (pos % align))
#define CHECK_HASH(z) (boost::hash_value(z))

#define REGISTRY_SIGNATURE (0xdeadbeef)

template <class IO, class Allocator>
class Registry {

    private:

       size_t region_size;

       // headers which are stamped per record
       const uint64_t magic = REGISTRY_SIGNATURE;

       size_t write_size;

       // registry entries always start at this alignment
       const size_t alignment = 128;

       // bump allocator
       off_t cursor;

       // In-Memory Registry
       std::list<db::registryrecord> reg_list;

       // Data-Structure for book-keeping snapshots
       std::map<size_t, vector<int>> _map;

       boost::shared_ptr<IO> _core;

       boost::shared_ptr<Allocator> _allocator;

    public:

       Registry(size_t size,
          boost::shared_ptr<IO> core, boost::shared_ptr<Allocator> alloc) :
          region_size(size), _core(core), _allocator(alloc) {

          // Search for the Signature
          const int start = SPACEMAPREGIONSIZE;
          off_t pos = ROUNDUP(start, alignment);
          while (pos < start + region_size) {
              // Fixed an issue where we passed member magic itself in the Read
              uint64_t val = 0;
             _core->Read(pos, (char*)&val, sizeof(magic));
              if (val != REGISTRY_SIGNATURE)
                  break;

              pos+=sizeof(magic);
             _core->Read(pos, (char*)&write_size, sizeof(write_size));
              auto rbuf = new char[write_size];
              db::registryrecord rec;
              pos+=sizeof(write_size);
             _core->Read(pos, rbuf, write_size);
              //BOOST_LOG_TRIVIAL(debug) << __func__ << ": " << CHECK_HASH(string(rbuf));
              // Note the String Constructor used.
              // In case the char pointer contains null characters
              rec.ParseFromString(std::string(rbuf, write_size));
              pos+=write_size;

              // Initializing in-memory registry
              reg_list.push_back(rec);

              cursor = ROUNDUP(pos, alignment);
              pos = cursor;
              BOOST_LOG_TRIVIAL(debug) << rec.ShortDebugString();
              BOOST_LOG_TRIVIAL(debug) << "registry next cursor location" << cursor;
              delete [] rbuf;
          }

          if (reg_list.empty()) {
              auto ans = _allocator->Allocate(region_size);
              BOOST_LOG_TRIVIAL(debug) << "Initializing Registry at offset : " << ans.first;
              cursor = ROUNDUP(ans.first, alignment);
          } else
              populateSnapList();
       }

       ~Registry() {
           reg_list.clear();
       }

       bool find(size_t id, db::registryrecord& rec) {
           BOOST_LOG_TRIVIAL(debug) << "Look Up key: " << id;

           if (reg_list.empty()) {
               BOOST_LOG_TRIVIAL(info) << "Registry is empty";
               return false;
           }

           for (auto &i : reg_list) {
               BOOST_LOG_TRIVIAL(trace) << __func__ << " id : " << i.key();
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

           db::registryrecord rec;
           assert(cursor % alignment == 0);
           rec.set_magic(REGISTRY_SIGNATURE);
           rec.set_version(0);
           rec.set_key(id);
           rec.set_issnap(false);
           rec.set_write_gen(cursor);
           rec.set_pkey(0);
           rec.set_phys_curr(cursor);
           rec.set_phys_next(0);
           rec.set_nr_elements(0);
           rec.set_type(db::registryrecord::LIST);

           std::string str;
           rec.SerializeToString(&str);
           BOOST_LOG_TRIVIAL(debug) << __func__ << ": " << rec.ShortDebugString();

           //Next position for new entry
           auto pos = cursor;
          _core->Write(pos, (char*)&magic, sizeof(magic));
           pos+=sizeof(magic);
           write_size = str.size();
          _core->Write(pos, (char*)&write_size, sizeof(write_size));
           pos+=sizeof(write_size);
          _core->Write(pos, str.c_str(), write_size);
           //BOOST_LOG_TRIVIAL(debug) << __func__ << ": " << CHECK_HASH(str);
           reg_list.push_back(rec);

           // Update cursor
           cursor = ROUNDUP(pos, alignment);
           BOOST_LOG_TRIVIAL(debug) << "registry next cursor location " << cursor;
       }

       bool reg_lookup(std::list<db::registryrecord>::iterator&  iter, uint64_t key) {

           if (reg_list.empty())
               return false;

           for (iter = reg_list.begin();
               (iter != reg_list.end()) && ((*iter).key() != key) ; iter++);

           return (iter != reg_list.end()) ? true : false;
       }

       void update(db::registryrecord& rec) {

           std::list<db::registryrecord>::iterator iter;
           if (!reg_lookup(iter, rec.key()))
               return;

           BOOST_LOG_TRIVIAL(debug) << __func__ << ": " << rec.key();

           *iter = rec;
           auto pos = (*iter).phys_curr();

           std::string str;
           (*iter).SerializeToString(&str);
           write_size = str.size();
           pos+=sizeof(magic);
          _core->Write(pos, (char*)&write_size, sizeof(write_size));
           pos+=sizeof(write_size);
          _core->Write(pos, str.c_str(), write_size);
       }

       void remove(size_t id) {

           std::list<db::registryrecord>::iterator iter;
           if (!reg_lookup(iter, id))
               return;

           BOOST_LOG_TRIVIAL(debug) << __func__ << ": " << id;

           (*iter).set_phys_next(0);
           (*iter).set_nr_elements(0);
           (*iter).set_write_gen(0);

           std::string str;
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

      void snapshot(size_t child_id, size_t parent_id) {

           std::list<db::registryrecord>::iterator iter;
           if (!reg_lookup(iter, parent_id)) {
               BOOST_LOG_TRIVIAL(error) << "Parent for Snapshot not found : " << parent_id;
               return;
           }

           // Create SnapShot Entry
           db::registryrecord rec;
           assert(cursor % alignment == 0);
           rec.set_magic(REGISTRY_SIGNATURE);
           rec.set_issnap(true);
           rec.set_version(0);
           rec.set_key(child_id);
           rec.set_write_gen(cursor);
           // Note : parent key stored here
           rec.set_pkey((*iter).key());
           rec.set_phys_next((*iter).phys_next());
           rec.set_nr_elements((*iter).nr_elements());
           rec.set_phys_curr(cursor);
           rec.set_type(db::registryrecord::LIST);

           std::string str;
           rec.SerializeToString(&str);
           BOOST_LOG_TRIVIAL(debug) << __func__ << ": " << rec.ShortDebugString();

           auto pos = cursor;
          _core->Write(pos, (char*)&magic, sizeof(magic));
           pos+=sizeof(magic);
           write_size = str.size();
          _core->Write(pos, (char*)&write_size, sizeof(write_size));
           pos+=sizeof(write_size);
          _core->Write(pos, str.c_str(), write_size);

           reg_list.push_back(rec);

           // Update cursor
           cursor = ROUNDUP(pos, alignment);
           BOOST_LOG_TRIVIAL(debug) << "registry next cursor location " << cursor;
      }

      void populateSnapList(void) {
         for (auto &i : reg_list)
             if (i.pkey())
                 _map[i.pkey()].push_back(i.nr_elements());
      }

      int GetSnapElements(size_t id) {
          return (_map.find(id) != _map.end()) ?
              *max_element(_map[id].begin(), _map[id].end()) : 0;
      }
};
